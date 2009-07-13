<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns="http://www.w3.org/1999/xhtml"
  xmlns:res="http://www.w3.org/2005/sparql-results#"
  exclude-result-prefixes="res xsl">

  <xsl:output
    method="text" 
    indent="yes"
    encoding="UTF-8" 
    doctype-public="-//W3C//DTD XHTML 1.0 Strict//EN"
    doctype-system="http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"
    omit-xml-declaration="yes" />

  <xsl:template name="boolean-result">
    <xsl:for-each select="res:boolean">
[] rdf:type rs:ResultSet ;
   rs:boolean "<xsl:value-of select="." />"^^xsd:boolean
    </xsl:for-each>
  </xsl:template>

  <xsl:template name="vb-result">
[] rdf:type rs:ResultSet ;
rs:resultVariable <xsl:for-each select="res:head/res:variable"><xsl:if test="position()!=1"><xsl:text>, </xsl:text></xsl:if>"<xsl:value-of select="@name"/>"</xsl:for-each>
<xsl:text> ;

</xsl:text>

  <xsl:variable name="order" select="res:results/@ordered" />
  <xsl:for-each select="res:results/res:result"> 
<xsl:text>
rs:solution [
</xsl:text>
<xsl:if test="$order='true'">
<xsl:text>  rs:index </xsl:text><xsl:value-of select="position()"/><xsl:text> ;
</xsl:text></xsl:if>
    <xsl:for-each select="res:binding"> 
     <xsl:variable name="name" select="@name" />
          <xsl:text>
  rs:binding [
    rs:variable "</xsl:text><xsl:value-of select="$name" /><xsl:text>" ;</xsl:text>
	<xsl:choose>
	  <xsl:when test="res:bnode/text()">
	    <!-- blank node value -->
	    <xsl:text>
    rs:value _:</xsl:text><xsl:value-of select="res:bnode/text()"/>
	  </xsl:when>
	  <xsl:when test="res:uri">
	    <!-- URI value -->
	    <xsl:variable name="uri" select="res:uri/text()"/>
	    <xsl:text>
    rs:value &lt;</xsl:text><xsl:value-of select="$uri"/><xsl:text>&gt; ;</xsl:text>
	  </xsl:when>

	  <xsl:when test="res:literal/@datatype">
	    <!-- datatyped literal value -->
	    <xsl:text>
    rs:value "</xsl:text><xsl:value-of select="res:literal/text()"/><xsl:text>"^^&lt;</xsl:text><xsl:value-of select="res:literal/@datatype"/><xsl:text>&gt; ;</xsl:text>
	  </xsl:when>
	  <xsl:when test="res:literal/@xml:lang">
	    <!-- lang-string -->
	    <xsl:text>
    rs:value "</xsl:text><xsl:value-of select="res:literal/text()"/><xsl:text>"@</xsl:text><xsl:value-of select="res:literal/@xml:lang"/><xsl:text> ;</xsl:text>

	  </xsl:when>
	  <xsl:when test="res:unbound">
	    <!-- unbound -->
	  </xsl:when>
	  <xsl:when test="string-length(res:literal/text()) != 0">
	    <!-- present and not empty -->
	    <xsl:text>
    rs:value "</xsl:text><xsl:value-of select="res:literal/text()"/><xsl:text>" ;</xsl:text>
	  </xsl:when>

	  <xsl:when test="string-length(res:literal/text()) = 0">
	    <!-- present and empty -->
	    <xsl:text>
    rs:value "" ;</xsl:text>
	  </xsl:when>
	  <xsl:otherwise>
<xsl:text>
    #unbound</xsl:text>
	  </xsl:otherwise>
	</xsl:choose>
    <xsl:text>
  ] ;
</xsl:text>
    </xsl:for-each>
    <xsl:text>
] ;
</xsl:text>

  </xsl:for-each>

  </xsl:template>


  <xsl:template match="res:sparql">
<xsl:text>
@prefix rs: &lt;http://www.w3.org/2001/sw/DataAccess/tests/result-set#&gt; .
@prefix rdf: &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#&gt; .
@prefix xsd: &lt;http://www.w3.org/2001/XMLSchema#&gt; .
</xsl:text>

<xsl:choose>
  <xsl:when test="res:boolean">
    <xsl:call-template name="boolean-result" />
  </xsl:when>

  <xsl:when test="res:results">
    <xsl:call-template name="vb-result" />

  </xsl:when>

</xsl:choose>

<xsl:text>
.
</xsl:text>
  </xsl:template>
</xsl:stylesheet>
