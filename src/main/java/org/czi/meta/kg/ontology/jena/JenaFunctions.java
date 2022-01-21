package org.czi.meta.kg.ontology.jena;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.jena.ontology.OntDocumentManager;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.ontology.Ontology;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.util.FileManager;
import org.apache.jena.vocabulary.RDF;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.util.*;

import static org.apache.jena.vocabulary.OWL2.NamedIndividual;
import static org.apache.jena.vocabulary.RDFS.label;
import static org.apache.jena.vocabulary.RDFS.subClassOf;


public class JenaFunctions {

    BufferedWriter writer;

    String meshCategoriesString = "A\tAnatomy\n" +
            "B\tOrganisms\n" +
            "C\tDiseases\n" +
            "D\tChemicals and Drugs\n" +
            "E\tAnalytical, Diagnostic and Therapeutic Techniques and Equipment\n" +
            "F\tPsychiatry and Psychology\n" +
            "G\tPhenomena and Processes\n" +
            "H\tDisciplines and Occupations\n" +
            "I\tAnthropology, Education, Sociology and Social Phenomena\n" +
            "J\tTechnology, Industry, Agriculture\n" +
            "K\tHumanities\n" +
            "L\tInformation Science\n" +
            "M\tNamed Groups\n" +
            "N\tHealth Care\n" +
            "V\tPublication Characteristics\n" +
            "Z\tGeographicals";
    Map<String, String> meshCategories = new HashMap<String, String>();

    public class UmlsMeshMetaRecord {
        public String meta = "";
        public String mesh = "";
        public String umls = "";
        public UmlsMeshMetaRecord(String meta, String mesh, String umls){
            this.mesh = mesh;
            this.meta = meta;
            this.umls = umls;
        }
    }

    public class PathElement {
        public Resource anchor;
        public Resource p1;
        public Resource p2;
        public Resource leaf;
        public PathElement(Resource anchor, Resource p1, Resource p2, Resource leaf) {
            this.anchor = anchor;
            this.p1 = p1;
            this.p2 = p2;
            this.leaf = leaf;
        }
        @Override
        public boolean equals(Object o){
            if (o == this) return true;
            if (!(o instanceof PathElement)) return false;
            PathElement pe = (PathElement) o;
            if( this.anchor==pe.anchor && this.p1==pe.p1 && this.p2==pe.p2 && this.leaf==pe.leaf ) return true;
            else return false;
        }

    }

    public static String META_URI = "http://meta.org/skos#";
    public static String MESHV_URI = "http://id.nlm.nih.gov/mesh/vocab#";
    public static String MESH_URI = "http://id.nlm.nih.gov/mesh/2020/";
    public static String SKOS_URI = "http://www.w3.org/2004/02/skos/core#";
    public static String UMLS_URI = "http://linkedlifedata.com/resource/umls/id/";
    public static String OBO_URI = "http://purl.obolibrary.org/obo/";
    public static String OBOINOWL_URI = "http://www.geneontology.org/formats/oboInOwl#";

    public static String ONTOLOGY_VERSION = "0.0.1";
    public static String ONTOLOGY_NAME = "META SKOS CONCEPT TAXONOMY";
    public static String ONTOLOGY_DESCRIPTION = "This is a SKOS representation that captures both hierarchical and " +
            "synonym information for concepts in the Meta Knowledge Graph.";


    List<UmlsMeshMetaRecord> records;
    Map<String, UmlsMeshMetaRecord> meshRecords;
    Map<String, UmlsMeshMetaRecord> umlsRecords;
    Map<String, UmlsMeshMetaRecord> metaRecords;
    Map<String, String> master_meta_lookup = new HashMap<String, String>();
    Map<String, Resource> treeLookup = new HashMap<String, Resource>();

    Property meshvBroaderDescriptor, meshvNarrowerDescriptor, meshvBroader, meshvNarrower,
            meshvConcept, meshvTerm,
            meshvScopeNote, meshvPreferredConcept, meshTreeNumber;
    Property skosInScheme, skosHasTopConcept, skosBroader, skosNarrower, skosDefiniton,
            skosAltLabel, skosExactMatch;
    Property iaoDefinition;
    Resource skosConceptScheme, skosConcept;
    OntModel m;
    OntModel bm;
    OntDocumentManager dm;
    int max_c_id = 0;

    public JenaFunctions() throws IOException {

        // create the base model
        this.bm = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF );

        for( String l : meshCategoriesString.split("\n") ) {
            String[] ll = l.split("\t");
            this.meshCategories.put(ll[0],ll[1]);
        }

        // create the reasoning model using the base
        this.m = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF, this.bm );
        this.dm = this.bm.getDocumentManager();

        ClassLoader classLoader = JenaFunctions.class.getClassLoader();

        Ontology ont = bm.createOntology( META_URI );
        ont.addImport( bm.createResource( SKOS_URI ) );
        ont.setLabel(ONTOLOGY_NAME, "en");
        ont.setComment(ONTOLOGY_DESCRIPTION, "en");
        ont.setVersionInfo(ONTOLOGY_VERSION);

        //File skos_ttl = new File(classLoader.getResource("skos.ttl").getFile());
        //dm.addAltEntry( SKOS_URI, skos_ttl.getPath() );
        //m.read( skos_ttl.getPath() );
        bm.setNsPrefix("skos", SKOS_URI);
        bm.setNsPrefix("", META_URI);
        bm.setNsPrefix("metaskos", META_URI);

        //File prov_ttl = new File(classLoader.getResource("prov.ttl").getFile());
        //dm.addAltEntry( PROV_URI, prov_ttl.getPath() );
        //m.read( prov_ttl.getPath(), "TURTLE" );

        this.skosExactMatch = m.getProperty(SKOS_URI+"exactMatch");
        this.skosBroader  = m.getProperty(SKOS_URI + "broader");
        this.skosNarrower  = m.getProperty(SKOS_URI + "narrower");
        this.skosDefiniton = m.getProperty(SKOS_URI + "definition");
        this.skosInScheme = m.getProperty(SKOS_URI + "inScheme");
        this.skosHasTopConcept  = m.getProperty(SKOS_URI + "hasTopConcept");
        this.skosAltLabel  = m.getProperty(SKOS_URI + "altLabel");

        this.iaoDefinition = m.getProperty(OBO_URI + "IAO_0000115");

        this.skosConcept = m.getResource(SKOS_URI + "Concept");
        this.skosConceptScheme = m.getResource(SKOS_URI + "ConceptScheme");

    }

    private  Set<Resource> executeSimpleSparql(Model model, String query) {
        Query query1 = QueryFactory.create(query);
        QueryExecution qexec1 = QueryExecutionFactory.create(query1, model);
        ResultSet results1 = qexec1.execSelect();
        Set<Resource> s = new HashSet<Resource>();
        for (; results1.hasNext(); ) {
            QuerySolution soln = results1.nextSolution();
            Resource d = soln.getResource("d");
            Resource x = soln.getResource("xref");
            //System.out.println(d+"\t"+x);
            s.add(d);
        }
        return s;
    }

    public void countMondoXrefs(File mondoFile) throws Exception {

        Model mondoModel = ModelFactory.createDefaultModel();
        mondoModel.read(new FileInputStream(mondoFile), null, "TTL");

        /*String[] searches = new String[] { "umls", "mesh", "snomedct", "DOID", "Orphanet" };
        String template = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?d ?xref \n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d skos:exactMatch ?xref .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  FILTER( regex(str(?xref), \"(XXXXX)\") )\n" +
                "} \n";
        for(String s : searches) {
            String query = template.replaceAll("XXXXX", s);
            Set<Resource> out = this.executeSimpleSparql(mondoModel, query);
            System.out.println("Count of all disease nodes with " + s + ": " + out.size());
        }

        String query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?d \n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "} \n";
        Set<Resource> out = this.executeSimpleSparql(mondoModel, query);
        System.out.println("Count of all disease nodes: " + out.size());*/

        String query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?d \n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?d rdfs:subClassOf+ ?t .\n" +
                "  ?t rdf:type owl:Restriction .\n" +
                "  ?t owl:onProperty obo:RO_0002573 .\n" +
                "  ?t owl:someValuesFrom obo:MONDO_0021136 .\n" +
                "} \n";
        Set<Resource> out = this.executeSimpleSparql(mondoModel, query);
        System.out.println("Count of all rare disease nodes: " + out.size());

        query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?d \n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?d rdfs:subClassOf+ ?t .\n" +
                "  ?t rdf:type owl:Restriction .\n" +
                "  ?t owl:onProperty obo:RO_0002573 .\n" +
                "  ?t owl:someValuesFrom obo:MONDO_0021136 .\n" +
                "  ?d skos:exactMatch ?xref .\n" +
                "  FILTER( regex(str(?xref), \"(umls)\") )\n" +
                "} \n";
        out = this.executeSimpleSparql(mondoModel, query);
        System.out.println("Count of all UMLS rare disease nodes: " + out.size());

    }

    public static class Options {

        @Option(name = "-mondoFile", usage = "MONDO File", required = true, metaVar = "MONDO-FILE")
        public File mondoFile;

    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        Options options = new Options();

        CmdLineParser parser = new CmdLineParser(options);

        try {

            parser.parseArgument(args);

        } catch (CmdLineException e) {

            System.err.println(e.getMessage());
            System.err.print("Arguments: ");
            parser.printSingleLineUsage(System.err);
            System.err.println("\n\n Options: \n");
            parser.printUsage(System.err);
            System.exit(-1);

        }

        JenaFunctions m2s = new JenaFunctions();
        m2s.countMondoXrefs(options.mondoFile);


    }
}
