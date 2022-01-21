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


public class QueryMondo {

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

    public QueryMondo() throws IOException {}

    public static class Options {

        @Option(name = "-mondoFile", usage = "MONDO File", required = true, metaVar = "MONDO-FILE")
        public File mondoFile;

        @Option(name = "-outFile", usage = "OUT File", required = true, metaVar = "OUT-FILE")
        public File outFile;

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

        FileWriter fw = new FileWriter(options.outFile);
        Writer w = new BufferedWriter(fw);

        Model mondoModel = ModelFactory.createDefaultModel();
        mondoModel.read(new FileInputStream(options.mondoFile), null, "TTL");

        String activeQuery = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "SELECT DISTINCT ?d ?dName ?synonym\n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0004976 .\n" +
                "  ?d oboInOwl:hasExactSynonym ?synonym \n" +

                //"  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                //"  FILTER NOT EXISTS { [] rdfs:subClassOf ?d }\n" +
                "} \n";

        Query query = QueryFactory.create(activeQuery);
        QueryExecution qexec = QueryExecutionFactory.create(query, mondoModel);
        ResultSet results = qexec.execSelect();

        //
        // Given a leaf, trace it's subClassOf hierarchy back up the hierarchy until it hits a MeSH-encoded node in
        // the hierarchy and  link it up  and then move to the next leaf. If a node has already been added, move to
        // the next leaf. Should behave like a depth first search.
        //
        HashSet<String> names = new HashSet<String>();
        for (; results.hasNext();) {
            QuerySolution soln = results.nextSolution();
            Resource d = soln.getResource("d");
            Literal dName = soln.getLiteral("dName");
            Literal synonym = soln.getLiteral("synonym");
            names.add(dName.getString());
            names.add(synonym.getString());
            System.out.println(d.getURI()+"\t"+dName.getString()+"\t"+synonym.getString());
        }

        for (String n : names) {
            w.write(n + '\n');
        }
        w.close();
    }
}
