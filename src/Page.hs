module Page where

import qualified Data.ByteString as B
import Data.Serialize (Serialize (..), putByteString, Get, Putter)
import Field (Field, FieldSpec, getField, fieldSpecSize, putField)
import Data.Word (Word16, Word32)

type TxId = Word32

-- ^ Transaction id representing Nothing in serialised row
txIdNone :: TxId
txIdNone = 0

-- ^ Minimum valid transaction id
txIdMin :: TxId
txIdMin = 1

data Row = Row
  { tmin :: TxId
  , tmax :: Maybe TxId
  , fields :: [Field]
  } deriving (Show, Eq)

getRowSize :: FieldSpec -> Word16
getRowSize
  = (8 +) . fieldSpecSize

getTmax :: Get (Maybe TxId)
getTmax = do
  n <- get
  return $ if n == txIdNone then Nothing else Just n

getRow :: FieldSpec -> Get Row
getRow fieldSpec
  = Row <$> get
        <*> getTmax
        <*> mapM getField fieldSpec

putTmax :: Putter (Maybe TxId)
putTmax Nothing
  = put txIdNone
putTmax (Just n)
  = put n

putRow :: Putter Row
putRow Row {..} = do
  put tmin
  putTmax tmax
  mapM_ putField fields

getRows :: Word16 -> FieldSpec -> Get [Row]
getRows usedSpace fieldSpec
  = helper 0
  where
    helper countParsed
      | usedSpace == getRowSize fieldSpec * countParsed
          = return []
      | usedSpace < getRowSize fieldSpec * countParsed
          = fail $  "Mismatch between parsed rows and reported space usage (usedSpace: " ++ show usedSpace ++ ", countParsed: " ++ show countParsed ++ ", rowSize: " ++ show (getRowSize fieldSpec) ++ ")"
      | otherwise = do
          row <- getRow fieldSpec
          rows <- helper (countParsed + 1)
          return $ row : rows

data Page = Page
  { usedSpace :: Word16
  , rows :: [Row]
  } deriving (Show)

-- ^ The size of a table page in bytes, including its header
pageSize :: Word16
pageSize
  = 1024

pageHeaderSize :: Word16
pageHeaderSize
  = 2

-- ^ The available data space of a space
pageSpace :: Word16
pageSpace
  = pageSize - pageHeaderSize

getPage :: FieldSpec -> Get Page
getPage fieldSpec = do
  usedSpace <- get
  rows <- getRows usedSpace fieldSpec
  return $ Page usedSpace rows

putPage :: Putter Page
putPage Page {..} = do
  put usedSpace
  mapM_ putRow rows
  putByteString $ B.replicate (fromIntegral $ pageSpace - usedSpace) 0
