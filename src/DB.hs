module DB where

import qualified Data.ByteString as B
import qualified StmContainers.Map as Map
import qualified Control.Concurrent.STM.Lock as L
import GHC.Conc (atomically)
import System.IO (Handle, openBinaryFile, IOMode(..), hSeek, SeekMode(..))

type Page = B.ByteString

data MemPage = MemPage
  { page :: Page
  , isDirty :: Bool
  }

type TableId = Int

type PageId = Int

type PageMap = Map.Map (TableId, PageId) MemPage

type LockMap = Map.Map TableId L.Lock

type HandleMap = Map.Map TableId Handle

pageSize :: Int
pageSize
  = 1024

getPage :: PageMap -> LockMap -> HandleMap -> TableId -> PageId -> IO MemPage
getPage pageMap lockMap handleMap tableId pageId = do
  pageOrLock <- atomically $ do
    page <- Map.lookup (tableId, pageId) pageMap
    case page of
      Just page' ->
        return $ Left page'
      Nothing -> do
        -- If page not loaded, load from disk. This is
        -- mutually-exclusive per table.
        lock <- Map.lookup tableId lockMap
        case lock of
          Nothing -> do
            lock <- L.new
            Map.insert lock tableId lockMap
            return $ Right lock
          Just lock' ->
            return $ Right lock'
  case pageOrLock of
    Left page ->
      return page
    Right lock -> do
      L.with lock $ do
        -- Check to see whether page has now been loaded
        page <- atomically $ Map.lookup (tableId, pageId) pageMap
        case page of
          Just page' ->
            return page'
          Nothing -> do
            -- Load page from disk
            -- Get file handle
            let
              getHandle = do
                handle <- openBinaryFile "test" ReadWriteMode
                atomically $ Map.insert handle tableId handleMap
                return handle
            handle' <- atomically $ Map.lookup tableId handleMap
            handle <- maybe getHandle return handle'
            hSeek handle AbsoluteSeek . toInteger $ pageSize * pageId
            page <- B.hGet handle pageSize
            let
              memPage
                = MemPage page False
            -- TODO: evict once page size reaches limit
            atomically $ Map.insert memPage (tableId, pageId) pageMap
            return $ memPage

f = 1
