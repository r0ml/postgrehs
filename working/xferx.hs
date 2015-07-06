
import Database.HDBC
import Database.HDBC.PostgreSQL
import Database.PostgreSQL.Simple.Copy
import System.Environment
import Data.Maybe (fromJust)

gcdx c = do
  z <- getCopyData c
  print (show z)
  gcdx c

xferx c x = do
  copy_ c ("copy " ++ x ++ " to stdout")
  gcdx c

main = do
  args <- getArgs
  let conns1 = head args
  let conns2 = (head . tail) args

  conn1 <- connectPostgreSQL conns1
  conn2 <- connectPostgreSQL conns2

--  run conn1 "create temp table tlist as select oid from pg_catalog.pg_class where relkind in ('r') and not relnamespace in (select oid from pg_namespace where nspname in ('pg_catalog','information_schema'))" []
  run conn1 "create temp table tlist as select oid from pg_catalog.pg_class where relkind in ('r') and relnamespace in (select oid from pg_namespace where nspname in ('account','config','document','global','log','mail','oauth','ownership','session','smartdoc'))" []
  run conn1 "create temp table constrs2 as select conname,confrelid::regclass,conrelid::regclass from pg_constraint where contype='f' and conrelid in (select oid from tlist)" []
  run conn1 "create temp table xlist as select oid::regclass from tlist where not oid::regclass in (select conrelid from constrs2)" []
  a <- quickQuery' conn1 "select * from xlist" []

  xferx conn1 (((fromSql::SqlValue->String) . head) (head a))

--  mapM_ ( print . (fromSql::SqlValue->String) . head)  a

