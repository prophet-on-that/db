module Test where

import Test.HUnit
import System.Random (randomIO)
import System.Directory (createDirectoryIfMissing)
import GHC.Conc (atomically, readTVar)
import Control.Exception (bracket)
import qualified StmContainers.Map as Map
import Data.Maybe (isJust)
import Control.Monad (forM_)
import Data.Serialize (encode, runPut)
import qualified Data.ByteString as B
import qualified Data.Set as Set
import Page
import Field
import DB

-- Test utilities

testDir
  = "test"

getTestDir = do
  f :: Float <- randomIO
  let
    dir
      = testDir ++ "/" ++ show f ++ "/"
  createDirectoryIfMissing True dir
  return dir

testCase :: String -> (DB -> Assertion) -> Test
testCase label test
  = TestLabel label . TestCase $ do
      testDir <- getTestDir
      bracket (newDB testDir) closeDB test

tests
  = TestList
      [ testCase "newDB -- initial state" $ \DB {..} -> do
          (atomically . readTVar) tableCounter >>= assertEqual "table counter" 0
          (atomically . readTVar) tableCounterHasChanged >>= assertEqual "table counter has changed" False
          (atomically . Map.size) pageMap >>= assertEqual "page map size" 0
          (atomically . Map.size) lockMap >>= assertEqual "lock map size" 0
          (atomically . Map.size) tableMap >>= assertEqual "table map size" 0
          (atomically . Map.size) fieldSpecMap >>= assertEqual "field spec map size" 0
          (atomically . readTVar) fieldSpecMapHasChanged >>= assertEqual "field spec map has changed" False
          (atomically . readTVar) txCounter >>= assertEqual "tx counter" txIdMin
          (atomically . Map.size) txMap >>= assertEqual "tx map size" 0

      , testCase "createTable -- create single table" $ \db@DB {..} -> do
          let
            fieldSpec
              = [FieldTypeInt32, FieldTypeBool]
          (tableId, _) <- createTable db fieldSpec
          assertEqual "table id" 0 tableId
          (atomically . readTVar) tableCounter >>= assertEqual "table counter" 1
          (atomically . readTVar) tableCounterHasChanged >>= assertEqual "table counter has changed" True
          Just TableData {..} <- atomically $ Map.lookup tableId tableMap
          assertEqual "table page count" 0 (pageCount tableHeader)
          assertEqual "table header is dirty" True dirtyHeader
          Just fieldSpec' <- atomically $ Map.lookup tableId fieldSpecMap
          assertEqual "" fieldSpec fieldSpec'
          (atomically . readTVar) fieldSpecMapHasChanged >>= assertEqual "" True
          lock <- atomically $ Map.lookup tableId lockMap
          assertBool "" $ isJust lock

      , testCase "createTable -- create two tables" $ \db@DB {..} -> do
          let
            fieldSpecs
              = [ [FieldTypeInt32, FieldTypeBool]
                , [FieldTypeBool, FieldTypeInt32]
                ]
          tableIds <- map fst <$> mapM (createTable db) fieldSpecs
          [0, 1] @=? tableIds
          (atomically . readTVar) tableCounter >>= assertEqual "" 2
          forM_ (zip tableIds fieldSpecs) $ \(tableId, fieldSpec) -> do
            Just fieldSpec' <- atomically $ Map.lookup tableId fieldSpecMap
            fieldSpec @=? fieldSpec'

      , testCase "createPage -- create single page" $ \db@DB {..} -> do
          let
            fieldSpec
              = [FieldTypeInt32]
          tableId <- fst <$> createTable db fieldSpec
          pageId <- atomically $ createPage db tableId [] 0
          Just TableData {..} <- atomically $ Map.lookup tableId tableMap
          True @=? dirtyHeader
          let
            TableHeader {..}
              = tableHeader
          1 @=? pageCount
          Just (MemPage Page {..} isDirty) <- atomically $ Map.lookup (tableId, pageId) pageMap
          True @=? isDirty
          0 @=? usedSpace
          [] @=? rows

      , TestLabel "tableHeaderSize" . TestCase $ do
          fromIntegral tableHeaderSize @=? (B.length . encode) (TableHeader 0)

      , TestLabel "pageSize" . TestCase $ do
          fromIntegral pageSize @=? (B.length . runPut . putPage) (Page 0 [])

      , testCase "beginTx" $ \db@DB {..} -> do
          txId <- atomically $ beginTx db
          txIdMin @=? txId
          (atomically . readTVar) txCounter >>= assertEqual "txCounter" (txIdMin + 1)
          Just Tx {..} <- atomically $ Map.lookup txId txMap
          Set.empty @=? writtenPages

      , testCase "insertRows -- insert single row into empty table" $ \db@DB {..} -> do
          let
            fieldSpec
              = [FieldTypeBool]
            rowData
              = [[FieldBool True]]
          tableId <- fst <$> createTable db fieldSpec
          txId <- atomically $ beginTx db
          insertRows db txId tableId rowData
          -- Test page count is correct
          Just (TableData TableHeader {..} _ _) <- atomically $ Map.lookup tableId tableMap
          1 @=? pageCount
          -- Test page contents
          Just (MemPage Page {..} isDirty) <- atomically $ Map.lookup (tableId, 0) pageMap
          True @=? isDirty
          getRowSize fieldSpec @=? usedSpace
          [Row txId Nothing (head rowData)] @=? rows

      , testCase "insertRows -- two pages created in empty table" $ \db@DB {..} -> do
          let
            fieldSpec
              = [FieldTypeBool]
            rowSize
              = getRowSize fieldSpec
            rowCountPerPage
              = pageSpace `div` rowSize
            rowData
              = replicate (1 + fromIntegral rowCountPerPage) [FieldBool True]
          tableId <- fst <$> createTable db fieldSpec
          txId <- atomically $ beginTx db
          insertRows db txId tableId rowData
          -- Test page count is correct
          Just (TableData TableHeader {..} _ _) <- atomically $ Map.lookup tableId tableMap
          2 @=? pageCount
          -- Test first page contents
          Just (MemPage Page {..} isDirty) <- atomically $ Map.lookup (tableId, 0) pageMap
          True @=? isDirty
          assertEqual "first page used space" usedSpace $ rowCountPerPage * rowSize
          fromIntegral rowCountPerPage @=? length rows
          -- Test second page contents
          Just (MemPage Page {..} isDirty) <- atomically $ Map.lookup (tableId, 1) pageMap
          True @=? isDirty
          assertEqual "second page used space" usedSpace rowSize
          1 @=? length rows
      ]

run
  = runTestTT tests
