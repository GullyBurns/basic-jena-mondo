package org.czi.meta.kg.ontology.jena;

import org.apache.jena.ontology.OntDocumentManager;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.util.*;


public class Extract_MONDO_Diseases {

    BufferedWriter writer;

    public static String UMLS_URI = "http://linkedlifedata.com/resource/umls/id/";
    public static String OBO_URI = "http://purl.obolibrary.org/obo/";
    public static String OBOINOWL_URI = "http://www.geneontology.org/formats/oboInOwl#";

    Property iaoDefinition;

    OntModel m;
    OntModel bm;
    OntDocumentManager dm;
    int max_c_id = 0;

    public Extract_MONDO_Diseases() throws IOException {

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

    public void queryDiseases(File mondoFile, String outstem) throws Exception {

        Model mondoModel = ModelFactory.createDefaultModel();
        mondoModel.read(new FileInputStream(mondoFile), null, "TTL");

        /* Get all disease nodes with additional information */
        String q0 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "SELECT DISTINCT ?mondo_id ?name\n" +
                "WHERE {\n" +
                "  ?mondo_id rdf:type owl:Class .\n" +
                "  ?mondo_id rdfs:label ?name .\n" +
                "  ?mondo_id rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "} \n";
        String[] q0_rcols = new String[] {"mondo_id"};
        String[] q0_lcols = new String[] {"name"};

        /* Get all disease nodes with additional information */
        String q1 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "SELECT DISTINCT ?mondo_id ?name ?parent_id ?parent_name\n" +
                "WHERE {\n" +
                "  ?mondo_id rdf:type owl:Class .\n" +
                "  ?mondo_id rdfs:label ?name .\n" +
                "  ?mondo_id rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?mondo_id rdfs:subClassOf ?parent_id .\n" +
                "  ?parent_id rdfs:label ?parent_name .\n" +
                "} \n";
        String[] q1_rcols = new String[] {"mondo_id", "parent_id"};
        String[] q1_lcols = new String[] {"name", "parent_name"};

        String q2 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "SELECT DISTINCT ?mondo_id ?xref \n" +
                "WHERE {\n" +
                "  ?mondo_id rdf:type owl:Class .\n" +
                "  ?mondo_id rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?mondo_id skos:exactMatch ?xref \n" +
                "} \n";
        String[] q2_rcols = new String[] {"mondo_id", "xref"};
        String[] q2_lcols = new String[] {};

        String q3 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "SELECT DISTINCT ?mondo_id ?synonym\n" +
                "WHERE {\n" +
                "  ?mondo_id rdf:type owl:Class .\n" +
                "  ?mondo_id rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?mondo_id oboInOwl:hasExactSynonym ?synonym \n" +
                "} \n";
        String[] q3_rcols = new String[] {"mondo_id"};
        String[] q3_lcols = new String[] {"synonym"};


        String q4 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?mondo_id \n" +
                "WHERE {\n" +
                "  ?mondo_id rdf:type owl:Class .\n" +
                "  ?mondo_id rdfs:label ?dName .\n" +
                "  ?mondo_id rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?mondo_id rdfs:subClassOf+ ?t .\n" +
                "  ?t rdf:type owl:Restriction .\n" +
                "  ?t owl:onProperty obo:RO_0002573 .\n" +
                "  ?t owl:someValuesFrom obo:MONDO_0021136 .\n" +
                "} \n";
        String[] q4_rcols = new String[] {"mondo_id"};
        String[] q4_lcols = new String[] {};

        String q5 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?mondo_id \n" +
                "WHERE {\n" +
                "  ?mondo_id rdf:type owl:Class .\n" +
                "  ?mondo_id rdfs:label ?dName .\n" +
                "  ?mondo_id rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?mondo_id rdfs:subClassOf+ ?t .\n" +
                "  ?t rdf:type owl:Restriction .\n" +
                "  ?t owl:onProperty obo:RO_0002573 .\n" +
                "  ?t owl:someValuesFrom obo:MONDO_0021136 .\n" +
                "} \n";
        String[] q5_rcols = new String[] {"mondo_id"};
        String[] q5_lcols = new String[] {};

        /* QUERY TO DETECT SIMPLE SUBTYPES OF DISEASES. NO REASON TO EXCLUDE THESE.
        String q5 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?d ?dName ?p ?pName\n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?d rdfs:subClassOf ?p .\n" +
                "  ?p rdfs:label ?pName .\n" +
                "  filter( regex(?dName, CONCAT(STR(?pName), \" [0-9]+\"))).\n" +
                "  FILTER NOT EXISTS { [] rdfs:subClassOf ?d }\n" +
                "} \n";
        String[] q5_rcols = new String[] {"d", "p"};
        String[] q5_lcols = new String[] {"dName", "pName"};
        */

        String[] names = new String[] {"disease", "parent", "xref", "synonym", "rare"};
        String[] queries = new String[] {q0, q1, q2, q3, q4};
        String[][] resources = new String[][] {q0_rcols, q1_rcols, q2_rcols, q3_rcols, q4_rcols};
        String[][] literals = new String[][] {q0_lcols, q1_lcols, q2_lcols, q3_lcols, q4_lcols};
        List<List<Map<String,String>>> output = new ArrayList<List<Map<String,String>>>();
        for(int i=0; i<queries.length; i++ ) {
            String q = queries[i];
            String[] rr = resources[i];
            String[] ll = literals[i];
            Query query1 = QueryFactory.create(q);
            QueryExecution qexec1 = QueryExecutionFactory.create(query1, mondoModel);
            ResultSet results1 = qexec1.execSelect();
            List<Map<String,String>> datalist = new ArrayList<>();
            System.out.println(q);
            for (; results1.hasNext(); ) {
                QuerySolution soln = results1.nextSolution();
                Map<String,String> datamap = new HashMap<String, String>();
                for(String r : rr) {
                    Resource res = soln.getResource(r);
                    datamap.put(r,res.toString());
                }
                for(String l : ll) {
                    Literal lit = soln.getLiteral(l);
                    datamap.put(l,lit.toString());
                }
                datalist.add(datamap);
            }
            output.add(datalist);
        }

        for(int i=0; i<queries.length; i++ ) {
            String n = names[i];
            List<Map<String,String>> l = output.get(i);

            List<String> headings = new ArrayList<String>(l.get(0).keySet());
            Collections.sort(headings);

            FileWriter w = new FileWriter(outstem + n + ".tsv");
            w.write(String.join("\t", headings));
            w.write("\n");
            for(Map<String,String> data: l) {
                List<String> line_data = new ArrayList<>();
                for(String key: headings) {
                    line_data.add(data.get(key));
                }
                w.write(String.join("\t", line_data));
                w.write("\n");
            }
            w.close();
        }

    }

    public static class Options {

        @Option(name = "-mondoFile", usage = "MONDO File", required = true, metaVar = "MONDO-FILE")
        public File mondoFile;

        @Option(name = "-outStem", usage = "Output Stem", required = true, metaVar = "MONDO-FILE")
        public String outStem;

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

        Extract_MONDO_Diseases mid = new Extract_MONDO_Diseases();
        mid.queryDiseases(options.mondoFile, options.outStem);

    }
}
