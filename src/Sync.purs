module Sync
  ( listenBrainzUrl
  , lbSync
  , lbSyncLoop
  ) where

import Prelude

import Control.Monad.Error.Class (throwError)
import Control.Monad.Rec.Class (forever)
import Data.Argonaut (decodeJson, parseJson)
import Data.Array (length, mapMaybe, null)
import Data.Either (Either(..))
import Data.Foldable (foldM)
import Data.Maybe (Maybe(..))
import Data.Set as Set
import Data.String (Pattern(..), stripPrefix)
import Data.Time.Duration (Milliseconds(..))
import Db
  ( Connection
  , getExistingTimestamps
  , upsertScrobble
  , withTransaction
  )
import Effect.Aff (Aff, delay, try)
import Effect.Aff.AVar (AVar)
import Effect.Aff.Retry
  ( RetryStatus(..)
  , exponentialBackoff
  , limitRetries
  , recovering
  )
import Effect.Class (liftEffect)
import Effect.Exception (error, message)
import Fetch (Method(GET), fetch)
import Http (apiTimeout, withTimeout)
import Log as Log
import Metrics as Metrics
import Types
  ( Listen(..)
  , ListenBrainzResponse(..)
  , Payload(..)
  )

-- | Result of processing a batch of listens/scrobbles.
type SyncResult =
  { added :: Int
  , hitCount :: Int
  , minTs :: Maybe Int
  }

-- | Process a batch of listens: check existence, insert new ones, track stats.
-- | When trackMinTs is true, the minimum timestamp is tracked (for cursor-based pagination).
-- | Existence is checked with one batch query up front instead of one query
-- | per listen; `seen` also guards against duplicate timestamps within a batch.
processTracks :: Connection -> Array Listen -> Boolean -> Aff SyncResult
processTracks conn listens trackMinTs = do
  existing <- getExistingTimestamps conn
    (mapMaybe (\(Listen l) -> l.listenedAt) listens)
  acc <- foldM step
    { added: 0, hitCount: 0, minTs: Nothing, seen: existing }
    listens
  pure { added: acc.added, hitCount: acc.hitCount, minTs: acc.minTs }
  where
  step
    :: { added :: Int
       , hitCount :: Int
       , minTs :: Maybe Int
       , seen :: Set.Set Int
       }
    -> Listen
    -> Aff
         { added :: Int
         , hitCount :: Int
         , minTs :: Maybe Int
         , seen :: Set.Set Int
         }
  step s l@(Listen { listenedAt: Just ts })
    | Set.member ts s.seen = do
        let nextMinTs = if trackMinTs then Just ts else s.minTs
        pure $ s { hitCount = s.hitCount + 1, minTs = nextMinTs }
    | otherwise = do
        upsertScrobble conn l
        let nextMinTs = if trackMinTs then Just ts else s.minTs
        pure $ s
          { added = s.added + 1
          , minTs = nextMinTs
          , seen = Set.insert ts s.seen
          }
  step s (Listen { listenedAt: Nothing }) = do
    Log.warn "Skipping scrobble without timestamp"
    pure s

-- | Build the ListenBrainz listens endpoint for a username.
listenBrainzUrl :: String -> String
listenBrainzUrl username = "https://api.listenbrainz.org/1/user/" <> username <>
  "/listens"

-- | Retry an action with exponential backoff, but only for transient errors.
-- | Parse errors and other permanent failures are not retried.
withRetry :: forall a. String -> Aff a -> Aff a
withRetry label action = recovering policy
  [ \_ err -> pure $ isTransientError err ]
  \(RetryStatus status) -> do
    when (status.iterNumber > 0)
      $ Log.warn
      $ label <> " failed, retry attempt " <> show status.iterNumber
    action
  where
  policy = exponentialBackoff (Milliseconds 1000.0) <> limitRetries 5
  isTransientError err = not isPermanent
    where
    msg = message err
    isPermanent =
      msg == "ListenBrainz: Failed to parse JSON response"
        || startsWith "ListenBrainz API returned status" msg

  startsWith prefix str = case stripPrefix (Pattern prefix) str of
    Just _ -> true
    Nothing -> false

fetchListenBrainzUrl :: String -> Aff String
fetchListenBrainzUrl url = withRetry "ListenBrainz fetch" do
  let headers = { "User-Agent": "corpus/1.0 (+https://sr.ht/~mtmn/corpus)" }
  fr <- withTimeout apiTimeout "ListenBrainz fetch" $ fetch url
    { method: GET, headers }
  if fr.status == 200 then
    withTimeout apiTimeout "ListenBrainz response" fr.text
  else
    throwError $ error $ "ListenBrainz API returned status " <> show fr.status

recordSyncSuccess :: String -> String -> Int -> Aff Unit
recordSyncSuccess slug source n = do
  liftEffect $ Metrics.incSyncRuns slug source "success"
  when (n > 0) $ liftEffect $ Metrics.incSyncScrobbles slug source n
  liftEffect $ Metrics.setSyncLastSuccess slug source

-- | True when every track in the batch was already present.
syncBatchDone :: Array Listen -> Int -> Boolean
syncBatchDone listens hitCount =
  hitCount == length listens && not (null listens)

-- | Log the result of processing one sync batch.
logSyncBatch :: String -> Int -> SyncResult -> Aff Unit
logSyncBatch source batchNum result =
  Log.info $ source <> " batch " <> show batchNum <> ": added "
    <> show result.added
    <> ", "
    <> show result.hitCount
    <> " already present."

-- | Perform one ListenBrainz synchronization pass.
lbSync :: Connection -> String -> String -> AVar Unit -> Aff Unit
lbSync conn username slug writeLock = void do
  Log.info $ "Starting ListenBrainz sync for " <> username
  result <- try $ fetchListenBrainzUrl
    (listenBrainzUrl username <> "?count=100")
  case result of
    Left err -> do
      Log.error $ "Sync fetch error: " <> message err
      liftEffect $ Metrics.incSyncRuns slug "listenbrainz" "error"
    Right body ->
      case parseJson body >>= decodeJson of
        Left err -> do
          Log.error $ "Sync parse error: " <> show err
          liftEffect $ Metrics.incSyncRuns slug "listenbrainz" "error"
        Right (ListenBrainzResponse { payload: Payload { listens } }) -> do
          syncResult <- withTransaction conn writeLock (processListens listens)
          logSyncBatch "ListenBrainz" 1 syncResult
          let allExist = syncBatchDone listens syncResult.hitCount
          if allExist || null listens then do
            when (syncResult.added > 0) $ Log.info $
              "ListenBrainz sync complete. Added " <> show syncResult.added <>
                " new scrobbles."
            recordSyncSuccess slug "listenbrainz" syncResult.added
          else do
            total <- paginateUntilDone 2 syncResult.minTs syncResult.added
            Log.info $ "ListenBrainz sync complete. Added " <> show total <>
              " new scrobbles."
            recordSyncSuccess slug "listenbrainz" total
  where
  paginateUntilDone batchNum minTs acc = case minTs of
    Nothing ->
      pure acc
    Just ts -> do
      Log.info $ "Fetching ListenBrainz batch " <> show batchNum <> " (before "
        <> show ts
        <> ")..."
      result <- try $ fetchListenBrainzUrl
        (listenBrainzUrl username <> "?count=100&max_ts=" <> show ts)
      case result of
        Left err -> do
          Log.error $ "Sync fetch error: " <> message err
          pure acc
        Right body ->
          case parseJson body >>= decodeJson of
            Left err -> do
              Log.error $ "Sync parse error: " <> show err
              pure acc
            Right (ListenBrainzResponse { payload: Payload { listens } }) -> do
              syncResult <- withTransaction conn writeLock
                (processListens listens)
              logSyncBatch "ListenBrainz" batchNum syncResult
              let allExist = syncBatchDone listens syncResult.hitCount
              if allExist || null listens then
                pure (acc + syncResult.added)
              else
                paginateUntilDone (batchNum + 1) syncResult.minTs
                  (acc + syncResult.added)

  processListens listens = processTracks conn listens true

-- | Run ListenBrainz synchronization every minute.
lbSyncLoop :: Connection -> String -> String -> AVar Unit -> Aff Unit
lbSyncLoop conn username slug writeLock = forever do
  delay (Milliseconds 60000.0)
  lbSync conn username slug writeLock
