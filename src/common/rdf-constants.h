#ifndef RDF_CONSTANTS_H
#define RDF_CONSTANTS_H

#define RDF_NAMESPACE    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
#define RDFS_NAMESPACE   "http://www.w3.org/2000/01/rdf-schema#"
#define OWL_NAMESPACE    "http://www.w3.org/2002/07/owl#"
#define XSD_NAMESPACE    "http://www.w3.org/2001/XMLSchema#"
#define DIRECT_NAMESPACE "http://triplestore.aktors.org/direct/#"

#define RDF_TYPE        RDF_NAMESPACE "type"
#define RDF_PROPERTY    RDF_NAMESPACE "Property"
#define RDF_RESOURCE    RDF_NAMESPACE "Resource"
#define OWL_OBJPROPERTY OWL_NAMESPACE "ObjectProperty"
#define OWL_DATPROPERTY OWL_NAMESPACE "DatatypeProperty"

#define RDFS_CLASS        RDFS_NAMESPACE "Class"
#define OWL_CLASS         OWL_NAMESPACE "Class"
#define RDFS_RESOURCE     RDFS_NAMESPACE "Resource"
#define RDFS_SUBCLASS     RDFS_NAMESPACE "subClassOf"
#define RDFS_SUBPROPERTY  RDFS_NAMESPACE "subPropertyOf"
#define RDFS_DOMAIN       RDFS_NAMESPACE "domain"
#define RDFS_RANGE        RDFS_NAMESPACE "range"
#define RDFS_LABEL        RDFS_NAMESPACE "label"

#define XSD_STRING	XSD_NAMESPACE "string"
#define XSD_INTEGER	XSD_NAMESPACE "integer"
#define XSD_DECIMAL	XSD_NAMESPACE "decimal"
#define XSD_FLOAT	XSD_NAMESPACE "float"
#define XSD_DOUBLE	XSD_NAMESPACE "double"
#define XSD_DATE	XSD_NAMESPACE "date"
#define XSD_TIME	XSD_NAMESPACE "time"
#define XSD_DATETIME	XSD_NAMESPACE "dateTime"
#define XSD_BOOLEAN	XSD_NAMESPACE "boolean"

#define INT_NS "http://id128.example.com/ontology/#"
#define INT_CLOSURE INT_NS "rdfsTransitiveClosure"
#define INT_DEPENDS INT_NS "dependsOn"

#define DIRECT_SUBCLASS    DIRECT_NAMESPACE "subClassOf"
#define DIRECT_SUBPROPERTY DIRECT_NAMESPACE "subPropertyOf"
#define DIRECT_TYPE        DIRECT_NAMESPACE "type"

#define FS_DEFAULT_GRAPH "default:"

#endif
