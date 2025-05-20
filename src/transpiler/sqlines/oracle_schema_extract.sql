/* PL/SQL script to extract database schema from an Oracle database.
   by SQLines, https://www.sqlines.com
   
   Run:
     $ sqlplus user/password@sid @oracle_schema_extract.sql <out_file> <schemas> <types> <filter>
     
     Parameters:
     
       <out_file> - Output file name (with directory), mandatory
       <types> - Comma separated list of types of objects to extract, default ALL (extract all types)
          Example: "TAB,CM,IDX" to extract tables, comments and indexes only
          Possible types:
            TAB - Tables
            CM  - Table and column comments
            IDX - Indexes
            VIE - Views
            MV  - Materialized views
            SEQ - Sequences
            SYN - Synonyms
            FN  - Functions
            SP  - Stored procedures
            PK  - Packages (specification and body)            
            TR  - Triggers
            
      Examples:
      
        # Extract all objects from all non-system schemas
        sqlplus user/password@sid @oracle_schema_extract.sql file.sql 
        
        # Extract schema SCOTT - tables, comments and indexes only for tables starting with E        
        sqlplus user/password@sid @oracle_schema_extract.sql file.sql SCOTT "tab,cm,idx" E%
*/

SET VERIFY OFF
SET SERVEROUTPUT ON
SET FEEDBACK OFF
SET HEADING OFF
SET PAGESIZE 0
SET LINESIZE 1000
SET LONG 1000000
SET LONGCHUNKSIZE 1000
SET TRIMSPOOL ON

SET TERMOUT OFF

----------------------------------------------------------------------------------------
-- Set the default values for parameters

COLUMN p_schemas  NEW_VALUE 2
COLUMN p_types    NEW_VALUE 3
COLUMN p_filter   NEW_VALUE 4

SELECT '' AS "p_schemas", '' AS "p_types", '' AS "p_filter" FROM dual WHERE rownum = 0;

DEFINE p_out_file = '&1'
DEFINE p_schemas = '&2'
DEFINE p_types = '&3'
DEFINE p_filter = '&4'

----------------------------------------------------------------------------------------
-- Schemas to extract (column separated list, or ALL to extract all non-system schemas)

DEFINE schema_filter = ''
COLUMN f NEW_VALUE schema_filter

SELECT 
  CASE WHEN UPPER(NVL('&p_schemas', 'ALL')) != 'ALL' 
    THEN 'AND owner IN (''' || REPLACE('&p_schemas', ',', ''',''') || ''')'
  END AS f 
FROM dual;

----------------------------------------------------------------------------------------
-- System schemas to exclude from extract (if p_schemas is ALL)

DEFINE schemas_exclude1 = ANONYMOUS,APEX_PUBLIC_USER,APPQOSSYS,AUDSYS,CTXSYS,DBSFWUSER,DBSNMP,DIP,DVF,DVSYS,EXFSYS,GGSYS,GSMADMIN_INTERNAL
DEFINE schemas_exclude2 = GSMCATUSER,GSMUSER,LBACSYS,MDDATA,MDSYS,OJVMSYS,OLAPSYS,ORACLE_OCM,ORDDATA,ORDPLUGINS,ORDSYS
DEFINE schemas_exclude3 = OUTLN,REMOTE_SCHEDULER_AGENT,SI_INFORMTN_SCHEMA,SPATIAL_CSW_ADMIN_USR,SPATIAL_WFS_ADMIN_USR
DEFINE schemas_exclude4 = SYS,SYSTEM,SYS$UMF,SYSBACKUP,SYSDG,SYSKM,SYSRAC,WMSYS,XDB,XS$NULL

DEFINE schema_exclude_filter = ''
COLUMN f NEW_VALUE schema_exclude_filter

SELECT 
  CASE WHEN UPPER(NVL('&p_schemas', 'ALL')) = 'ALL' 
    THEN 'AND owner NOT IN (''' || REPLACE('&schemas_exclude1,&schemas_exclude2,&schemas_exclude3,&schemas_exclude4', ',', ''',''') || ''')'
  END AS f 
FROM dual;

----------------------------------------------------------------------------------------
-- Object name filters 

DEFINE object_filter = ''
COLUMN f NEW_VALUE object_filter

SELECT CASE WHEN '&p_filter' != NVL('', '%') THEN 'AND object_name LIKE ''' || '&p_filter' || '''' END AS f FROM dual;

----------------------------------------------------------------------------------------
-- Extracting objects

SET TERMOUT ON

SPOOL &p_out_file

-- Setting the delimiter after each statement
BEGIN
 DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
 DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
END;
/

-- Extract tables (if selected)
SELECT dbms_metadata.get_ddl('TABLE', object_name, owner)
FROM (
  SELECT owner, table_name AS object_name FROM all_tables 
) t
WHERE (INSTR(UPPER('&p_types'), 'TAB') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter;

-- Extract table and column comments (if selected)
SELECT dbms_metadata.get_dependent_ddl('COMMENT', object_name, owner)
FROM (
  SELECT owner, table_name AS object_name FROM all_tab_comments WHERE comments IS NOT NULL
  UNION
  SELECT owner, table_name AS object_name FROM all_col_comments WHERE comments IS NOT NULL
) t  
WHERE (INSTR(UPPER('&p_types'), 'CM') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL') 
  &schema_filter
  &schema_exclude_filter
  &object_filter;
  
-- Extract indexes (if selected)
SELECT dbms_metadata.get_ddl('INDEX', index_name, owner)
FROM (
  SELECT owner, table_name AS object_name, index_name FROM all_indexes 
) t
WHERE (INSTR(UPPER('&p_types'), 'IDX') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter;
  
-- Extract views (if selected)
SELECT dbms_metadata.get_ddl('VIEW', object_name, owner)
FROM (
  SELECT owner, view_name AS object_name FROM all_views 
) t
WHERE (INSTR(UPPER('&p_types'), 'VIE') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter;
  
-- Extract materialized views (if selected)
SELECT dbms_metadata.get_ddl('MATERIALIZED_VIEW', object_name, owner)
FROM (
  SELECT owner, mview_name AS object_name FROM all_mviews 
) t
WHERE (INSTR(UPPER('&p_types'), 'MV') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter;
  
-- Extract sequences (if selected)
SELECT dbms_metadata.get_ddl('SEQUENCE', object_name, owner)
FROM (
  SELECT sequence_owner AS owner, sequence_name AS object_name FROM all_sequences 
) t
WHERE (INSTR(UPPER('&p_types'), 'SEQ') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter; 

-- Extract synonyms (if selected)
SELECT dbms_metadata.get_ddl('SYNONYM', object_name, owner)
FROM (
  SELECT owner, synonym_name AS object_name FROM all_synonyms 
) t
WHERE (INSTR(UPPER('&p_types'), 'SYN') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter;   
  
-- Extract functions (if selected)
SELECT dbms_metadata.get_ddl('FUNCTION', object_name, owner)
FROM (
  SELECT owner, object_name FROM all_objects WHERE object_type = 'FUNCTION' 
) t
WHERE (INSTR(UPPER('&p_types'), 'FN') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter; 
  
-- Extract stored procedures (if selected)
SELECT dbms_metadata.get_ddl('PROCEDURE', object_name, owner)
FROM (
  SELECT owner, object_name FROM all_objects WHERE object_type = 'PROCEDURE' 
) t
WHERE (INSTR(UPPER('&p_types'), 'SP') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter; 
  
-- Extract packages (if selected)
SELECT dbms_metadata.get_ddl('PACKAGE', object_name, owner)
FROM (
  SELECT owner, object_name FROM all_objects WHERE object_type = 'PACKAGE' 
) t
WHERE (INSTR(UPPER('&p_types'), 'PK') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter; 

-- Extract triggers (if selected)
SELECT dbms_metadata.get_ddl('TRIGGER', trigger_name, owner)
FROM (
  SELECT owner, table_name AS object_name, trigger_name FROM all_triggers 
) t
WHERE (INSTR(UPPER('&p_types'), 'TR') > 0 OR UPPER(NVL('&p_types', 'ALL')) = 'ALL')
  &schema_filter
  &schema_exclude_filter
  &object_filter;   

SPOOL OFF

EXIT