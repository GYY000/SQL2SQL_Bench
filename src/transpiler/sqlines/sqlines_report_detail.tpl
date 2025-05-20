<style type="text/css">
body { font-family: verdana; }
h1 { font-family: trebuchet ms, verdana; font-size: 20px; color: #000000; border-bottom:1px solid grey; }
h2 { font-family: trebuchet ms, verdana; font-size: 18px; color: #000000; border-bottom:1px solid grey; }
p { font-size: 13px; }
table { font-size: 13px; border: 1px solid; border-collapse:collapse; }
.report_tab tr:hover td { background-color: #A4BBFE; }
th { background: #FFFFFF; padding: 0px 10px 0px 10px; border: 1px solid grey; }
td { padding: 0px 10px 0px 10px; border: 1px solid grey; }
.td_warn { background:yellow; }
.td_error { background:lightpink; }
a:link { text-decoration: none; color: #0000A0;}
a:visited { text-decoration: none; color: #0000A0;}
a:active { text-decoration: none; color: #0000A0;}
a:hover { text-decoration: none; color: #800517;}
pre { white-space: pre-wrap; } 
.token_kw { color:blue }
.token_str { color:green }
.token_num { color:brown }
.token_dtype { color:maroon  }
.token_symb { color:darkblue }
.token_comment { color:grey }
.token_func { color:blue }
.token_udf { color:#8B0000 }
.token_ok { background-color:#E0FFD2 }
.token_error { background-color:#FFCCCB }
.token_warn { background-color:#FFF380 }

.tooltip { position: relative; display: inline-block; cursor: pointer; }
.tooltip .tooltiptext {
  visibility: hidden;
  background-color: #fff;
  color: #555;
  font-family: verdana;
  font-size: 11px; 
  border-style: ridge;
  border-radius: 6px;
  padding: 5px 5px;
  position: absolute;
  z-index: 1;
  left: 100%;           /* tooltip is placed on the right without any padding */
  white-space: normal;
  top: 50%;
  opacity: 0;
  transition: opacity 0.3s;
  width: max-content;
  margin-left: -10px;  /* tooltip sligthly overlaps on the right so we can easily go to link */
  user-select: none;
}
.tooltip:hover .tooltiptext { visibility: visible; opacity: 1; }

</style>

<!------------------------------->
<?ifexists:top_statements_by_type_table?>
<h2>Top-Level Statements by Type</h2>

<p>Standalone top-level SQL statements by type excluding nested statements that appear inside other statements 
(CREATE TABLE or INSERT inside CREATE PROCEDURE i.e.):</p>
<?top_statements_by_type_table?>
<?/ifexists:top_statements_by_type_table?>

<!------------------------------->
<?ifexists:statements_table?>
<h2>All SQL Statements</h2>

<p>Note that some statements can be nested within others (CREATE TABLE or INSERT inside CREATE PROCEDURE i.e. so issues, lines and other metrics cannot be summed): </p>
<?statements_table?>
  <?ifexists:crtab_stmt_table?><p>CREATE TABLE statements details:</p><?crtab_stmt_table?><?/ifexists:crtab_stmt_table?>
  <?ifexists:alttab_stmt_table?><p>ALTER TABLE statements details:</p><?alttab_stmt_table?><?/ifexists:alttab_stmt_table?>
  <?ifexists:cridx_stmt_table?><p>CREATE INDEX statements details:</p><?cridx_stmt_table?><?/ifexists:cridx_stmt_table?>
  <?ifexists:crview_stmt_table?><p>CREATE VIEW statements details:</p><?crview_stmt_table?><?/ifexists:crview_stmt_table?>
  <?ifexists:select_stmt_table?><p>SELECT statements details:</p><?select_stmt_table?><?/ifexists:select_stmt_table?>
  <?ifexists:udf_stmt_table?><p>CREATE FUNCTION statements details:</p><?udf_stmt_table?><?/ifexists:udf_stmt_table?>
  <?ifexists:crproc_stmt_table?><p>CREATE PROCEDURE statements details:</p><?crproc_stmt_table?><?/ifexists:crproc_stmt_table?>
  <?ifexists:othersql_stmt_table?><p>Other SQL statements details:</p><?othersql_stmt_table?><?/ifexists:othersql_stmt_table?>
<?/ifexists:statements_table?>

<!------------------------------->
<?ifexists:datatypes_table?>
<h2>Data Types</h2>

<p>All built-in data types:</p><?datatypes_table?>
  <?ifexists:datatype_dtl_table?><p>Built-in data type details:</p><?datatype_dtl_table?><?/ifexists:datatype_dtl_table?>
  <?ifexists:udt_datatypes_table?><p>All derived and user-defined (UDT) data types:</p><?udt_datatypes_table?><?/ifexists:udt_datatypes_table?>
<?/ifexists:datatypes_table?>

<!------------------------------->
<?ifexists:expressions_table?>
<h2>Expressions</h2>

<p>Specific SQL language elements and expressions:</p><?expressions_table?>
  
<?/ifexists:expressions_table?>

<!------------------------------->
<?ifexists:builtin_func_table?>
<h2>Functions</h2>

<p>All built-in functions:</p><?builtin_func_table?>
  <?ifexists:builtin_func_dtl_table?><p>Built-in function details:</p><?builtin_func_dtl_table?><?/ifexists:builtin_func_dtl_table?>
<?/ifexists:builtin_func_table?>

<!------------------------------->
<?ifexists:seq_dtl_table?>
<h2>Sequences</h2>

<p>Sequence options:</p><?seq_dtl_table?>
  <?ifexists:seq_ref_table?><p>Sequence references:</p><?seq_ref_table?><?/ifexists:seq_ref_table?>
<?/ifexists:seq_dtl_table?>

<!------------------------------->
<?ifexists:system_proc_table?>
<h2>Procedures</h2>

<p>All system procedures calls:</p><?system_proc_table?>
<p>System procedure call details:</p><?system_proc_dtl_table?>
<?/ifexists:system_proc_table?>

<!------------------------------->
<?ifexists:pl_statements?>
<h2>Procedural Language Statements</h2>

<p>All procedural SQL statements and constructs:</p><?pl_statements?>
  <?ifexists:pl_statements_exceptions?><p>Predefined exception handlers:</p><?pl_statements_exceptions?><?/ifexists:pl_statements_exceptions?>
<?/ifexists:pl_statements?>

<!------------------------------->
<?ifexists:packages?>
<h2>Built-in Packages</h2>

<p>All built-in packages:</p><?packages?>
  <?ifexists:pkg_statements_items?><p>Built-in packages functions, procedures and constants:</p><?pkg_statements_items?><?/ifexists:pkg_statements_items?>
<?/ifexists:packages?>

<!------------------------------->
<?ifexists:conversion_errors?>
<a name="conversion_errors"></a>
<h2>Conversion Errors</h2>

<p>Conversion errors that require manual refinement or redesign:</p><?conversion_errors?>
<?/ifexists:conversion_errors?>

<!------------------------------->
<?ifexists:conversion_issues?>
<a name="conversion_issues"></a>
<h2>Conversion Issues</h2>

<p>Conversion issues that require review:</p><?conversion_issues?>
<?/ifexists:conversion_issues?>

<!------------------------------->
<?ifexists:conversion_warnings?>
<a name="conversion_warnings"></a>
<h2>Conversion Warnings</h2>

<p>Conversion warnings that require review (conversion can be ok but some functionality can be restricted):</p><?conversion_warnings?>
<?/ifexists:conversion_warnings?>

<!------------------------------->
<?ifexists:syntax_errors_unexpected_tokens?>
<a name="conversion_syntax_errors"></a>
<h2>Syntax Errors or Unexpected Tokens</h2>

<p>Syntax errors or unexpected tokens in the input file(s):</p><?syntax_errors_unexpected_tokens?>
<?/ifexists:syntax_errors_unexpected_tokens?>

<!------------------------------->
<?ifexists:undefined_tokens?>
<a name="conversion_undefined_tokens"></a>
<h2>Undefined Tokens</h2>

<p>Parsed tokens for which conversion is undefined:</p><?undefined_tokens?>
<?/ifexists:undefined_tokens?>
