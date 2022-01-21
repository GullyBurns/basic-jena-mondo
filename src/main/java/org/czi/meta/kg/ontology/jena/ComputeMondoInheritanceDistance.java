package org.czi.meta.kg.ontology.jena;

import java.io.*;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.jena.ontology.*;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.util.FileManager;
import org.apache.jena.vocabulary.RDF;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import static org.apache.jena.vocabulary.RDFS.label;
import static org.apache.jena.vocabulary.OWL2.NamedIndividual;
import static org.apache.jena.vocabulary.RDFS.subClassOf;


public class ComputeMondoInheritanceDistance {

    BufferedWriter writer;

    public static String UMLS_URI = "http://linkedlifedata.com/resource/umls/id/";
    public static String OBO_URI = "http://purl.obolibrary.org/obo/";
    public static String OBOINOWL_URI = "http://www.geneontology.org/formats/oboInOwl#";

    Property iaoDefinition;

    OntModel m;
    OntModel bm;
    OntDocumentManager dm;
    int max_c_id = 0;

    public ComputeMondoInheritanceDistance() throws IOException {

        // create the base model
        this.bm = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF );

        // create the reasoning model using the base
        this.m = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF, this.bm );
        this.dm = this.bm.getDocumentManager();

        ClassLoader classLoader = MeshToSkos.class.getClassLoader();

        //File prov_ttl = new File(classLoader.getResource("prov.ttl").getFile());
        //dm.addAltEntry( PROV_URI, prov_ttl.getPath() );
        //m.read( prov_ttl.getPath(), "TURTLE" );
        this.iaoDefinition = m.getProperty(OBO_URI + "IAO_0000115");

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

        String[] searches = new String[] { "umls", "mesh", "snomedct", "DOID", "Orphanet" };
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
            System.out.println( query );
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
        System.out.println("Count of all disease nodes: " + out.size());
    }

    public void computeSiblings(File mondoFile) throws Exception {

        Model mondoModel = ModelFactory.createDefaultModel();
        mondoModel.read(new FileInputStream(mondoFile), null, "TTL");

        String[] searches = new String[] { "umls", "mesh", "snomedct", "DOID", "Orphanet" };
        String query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?d1 ?d1Name ?d2 ?d2Name\n" +
                "WHERE {\n" +
                "  ?d1 rdf:type owl:Class .\n" +
                "  ?d1 rdfs:label ?d1Name .\n" +
                "  ?d1 rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?d1 rdfs:subClassOf ?p .\n" +
                "  ?d2 rdf:type owl:Class .\n" +
                "  ?d2 rdfs:label ?d2Name .\n" +
                "  ?d2 rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?d2 rdfs:subClassOf ?p .\n" +
                "} \n";
        System.out.println( query );

        Query query1 = QueryFactory.create(query);
        QueryExecution qexec1 = QueryExecutionFactory.create(query1, mondoModel);
        ResultSet results1 = qexec1.execSelect();
        Set<Resource> s = new HashSet<Resource>();
        for (; results1.hasNext(); ) {
            QuerySolution soln = results1.nextSolution();
            Resource d1 = soln.getResource("d1");
            Resource d2 = soln.getResource("d2");
            Literal d1Name = soln.getLiteral("d1Name");
            Literal d2Name = soln.getLiteral("d2Name");
            System.out.println(d1+"\t"+d2);
        }

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

        ComputeMondoInheritanceDistance mid = new ComputeMondoInheritanceDistance();
        mid.computeSiblings(options.mondoFile);

    }
}
