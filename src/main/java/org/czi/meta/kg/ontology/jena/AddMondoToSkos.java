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

public class AddMondoToSkos {

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

    public static String META_URI = "http://meta.org/skos#";
    public static String MESH_URI = "http://id.nlm.nih.gov/mesh/vocab#";
    public static String SKOS_URI = "http://www.w3.org/2004/02/skos/core#";
    public static String UMLS_URI = "http://linkedlifedata.com/resource/umls/id/";

    public static String ONTOLOGY_VERSION = "0.0.1";
    public static String ONTOLOGY_NAME = "META SKOS CONCEPT TAXONOMY";
    public static String ONTOLOGY_DESCRIPTION = "This is a SKOS representation that captures both hierarchical and " +
            "synonym information for concepts in the Meta Knowledge Graph.";

    List<UmlsMeshMetaRecord> records;
    Map<String, UmlsMeshMetaRecord> meshRecords;
    Map<String, UmlsMeshMetaRecord> umlsRecords;
    Map<String, UmlsMeshMetaRecord> metaRecords;
    Map<Resource, String> mesh_meta_lookup = new HashMap<Resource, String>();
    Map<String, Resource> treeLookup = new HashMap<String, Resource>();

    Property meshvBroaderDescriptor, meshvBroader, meshvConcept, meshvTerm,
            meshvScopeNote, meshvPreferredConcept, meshTreeNumber;
    Property skosInScheme, skosHasTopConcept, skosBroader, skosNarrower, skosDefiniton,
            skosAltLabel, skosExactMatch;
    Resource skosConceptScheme, skosConcept;
    OntModel m;
    OntModel bm;
    OntDocumentManager dm;
    int max_c_id = 0;

    public AddMondoToSkos(){
        // create the base model
        this.bm = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF );

        // create the reasoning model using the base
        this.m = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF, this.bm );
        this.dm = this.bm.getDocumentManager();

        ClassLoader classLoader = AddMondoToSkos.class.getClassLoader();

        Ontology ont = bm.createOntology( META_URI );
        ont.addImport( bm.createResource( SKOS_URI ) );
        ont.setLabel(ONTOLOGY_NAME, "@en");
        ont.setComment(ONTOLOGY_DESCRIPTION, "@en");
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

        this.skosConcept = m.getResource(SKOS_URI + "Concept");
        this.skosConceptScheme = m.getResource(SKOS_URI + "ConceptScheme");

    }

    private String getMetaUriFromMesh(Resource mesh) {

        if( this.mesh_meta_lookup.containsKey(mesh) ){
            return this.mesh_meta_lookup.get(mesh);
        }

        String curi = mesh.getURI().substring(mesh.getNameSpace().length());
        String c_id = META_URI+"concept_"+this.max_c_id;
        if(this.meshRecords.containsKey(curi) && this.meshRecords.get(curi).meta.length() > 0) {
            c_id = META_URI+"concept_" + this.meshRecords.get(curi).meta;
        } else {
            this.max_c_id++;
        }
        this.mesh_meta_lookup.put(mesh, c_id);

        return c_id;

    }


    private Resource meshDescriptorToSkosConcept(Resource meshDesc) throws Exception {

        Resource prefConcept = meshDesc.getProperty(this.meshvPreferredConcept).getObject().asResource();
        Resource conceptScheme = this.getConceptScheme(meshDesc);
        Resource metaConcept = this.addMeshConcept(prefConcept, conceptScheme);
        this.addExactMatches(metaConcept, meshDesc);

        this.addNarrowerConcepts(metaConcept, prefConcept, conceptScheme);

        StmtIterator nIt = prefConcept.listProperties(m.getProperty(MESH_URI + "broaderConcept"));
        Set<String> unanchoredConcepts = new HashSet<String>();
        while(nIt.hasNext()) {
            Statement s = nIt.nextStatement();
            Resource nConcept = s.getObject().as(Resource.class);
            unanchoredConcepts.add(nConcept.getProperty(label).getLiteral().getLexicalForm());
            System.out.println("Unanchored concepts without discernable parent: " +
                    nConcept.getProperty(label).getLiteral().getLexicalForm());
        }

        StmtIterator pIt = meshDesc.listProperties(this.meshvBroader);
        while(pIt.hasNext()){
            Statement s = pIt.nextStatement();
            Resource meshParent = s.getObject().as(Resource.class);
            Resource metaParent = this.addMeshConcept(meshParent, conceptScheme);
            this.bm.add(metaConcept, this.skosBroader, metaParent);
            this.bm.add(metaParent, this.skosNarrower, metaConcept);
        }

        return metaConcept;

    }

    private Resource getConceptScheme(Resource meshDesc) {
        Resource tree = meshDesc.getProperty(this.meshTreeNumber).getObject().asResource();
        String treeValue = tree.getProperty(label).getLiteral().getLexicalForm();
        String topTree = treeValue;
        if( treeValue.indexOf('.') != -1) {
            topTree = treeValue.substring(0, treeValue.indexOf('.'));
        }
        Resource topConcept = this.treeLookup.get(topTree);
        String cs_id = topConcept.getURI().replaceAll("concept_", "scheme_");
        Resource metaConceptSchema = this.bm.getResource(cs_id);
        return metaConceptSchema;
    }

    void addNarrowerConcepts(Resource metaConcept, Resource meshConcept, Resource conceptScheme) {
        StmtIterator nIt = meshConcept.listProperties(m.getProperty(MESH_URI + "narrowerConcept"));
        while(nIt.hasNext()) {
            Statement s = nIt.nextStatement();
            Resource nConcept = s.getObject().as(Resource.class);
            Resource nc = this.addMeshConcept(nConcept, conceptScheme);
            this.bm.add(metaConcept, this.skosNarrower, nc);
            this.bm.add(nc, this.skosBroader, metaConcept);
            this.addNarrowerConcepts(nc, nConcept, conceptScheme);
        }
    }

    void addExactMatches(Resource metaConcept, Resource mesh) {
        String meshId = mesh.getURI().substring(mesh.getURI().lastIndexOf('/')+1);
        this.bm.add(metaConcept, this.skosExactMatch, mesh);
        if (this.meshRecords.containsKey(meshId) && this.meshRecords.get(meshId).umls.length() > 0) {
            Resource umlsMatch = m.getResource(UMLS_URI + this.meshRecords.get(meshId).umls);
            this.bm.add(metaConcept, this.skosExactMatch, umlsMatch);
        }
    }

    Resource addMeshConcept(Resource meshConcept, Resource conceptScheme) {
        Resource metaConcept = this.addMeshConcept(meshConcept);
        metaConcept.addProperty(this.skosInScheme, conceptScheme);
        return metaConcept;
    }

    Resource addMeshConcept(Resource meshConcept) {
        String c_id = getMetaUriFromMesh(meshConcept);
        Resource metaConcept = this.bm.getResource(c_id);

        // return if details for this concept already filled in.
        if( metaConcept.hasProperty(label) )
            return metaConcept;

        this.addExactMatches(metaConcept, meshConcept);

        metaConcept.addProperty(RDF.type, NamedIndividual);
        metaConcept.addProperty(RDF.type, this.skosConcept);
        metaConcept.addLiteral(label, meshConcept.getProperty(label).getLiteral());
        if( meshConcept.getProperty(this.meshvScopeNote) != null ) {
            Literal scopeNote = meshConcept.getProperty(this.meshvScopeNote).getLiteral();
            metaConcept.addLiteral(this.skosDefiniton, scopeNote);
        }

        Set<Literal> altLabels = new HashSet<Literal>();
        StmtIterator tIt = meshConcept.listProperties(this.meshvTerm);
        while(tIt.hasNext()) {
            Statement s2 = tIt.nextStatement();
            Resource meshTerm = s2.getObject().as(Resource.class);
            StmtIterator lIt = meshTerm.listProperties(label);
            while (lIt.hasNext()) {
                Statement s3 = lIt.nextStatement();
                if(!altLabels.contains(s3.getObject().asLiteral())) {
                    altLabels.add(s3.getObject().asLiteral());
                }
            }
        }
        for(Literal l : altLabels){
            this.bm.addLiteral(metaConcept, this.skosAltLabel, l);
        }

        return metaConcept;

    }


    public void buildSkosFromMesh(File meshFile) throws Exception {

        Model mesh_schema = FileManager.get().loadModel(MESH_URI);
        Model data = FileManager.get().loadModel(meshFile.getPath());
        Reasoner reasoner = ReasonerRegistry.getRDFSReasoner();
        reasoner = reasoner.bindSchema(mesh_schema);
        InfModel meshModel = ModelFactory.createInfModel(reasoner, data);

        this.meshvBroaderDescriptor = meshModel.getProperty(MESH_URI+"broaderDescriptor");
        this.meshvBroader = meshModel.getProperty(MESH_URI+"broader");
        this.meshvScopeNote = meshModel.getProperty(MESH_URI+"scopeNote");
        this.meshvPreferredConcept = meshModel.getProperty(MESH_URI+"preferredConcept");
        this.meshvConcept = meshModel.getProperty(MESH_URI+"concept");
        this.meshvTerm = meshModel.getProperty(MESH_URI+"term");
        this.meshTreeNumber = meshModel.getProperty(MESH_URI+"treeNumber");

        String treeQuery = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>\n" +
                "PREFIX mesh: <http://id.nlm.nih.gov/mesh/>\n" +
                "PREFIX mesh2020: <http://id.nlm.nih.gov/mesh/2020/>\n" +
                "PREFIX mesh2019: <http://id.nlm.nih.gov/mesh/2019/>\n" +
                "PREFIX mesh2018: <http://id.nlm.nih.gov/mesh/2018/>\n" +
                "SELECT ?d ?dName ?tName\n" +
                "WHERE {\n" +
                "  ?d a meshv:TopicalDescriptor .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d meshv:treeNumber ?t .\n" +
                "  ?t rdfs:label ?tName .\n" +
                "  FILTER( regex(?tName, \"^[A-Z][0-9]+$\") )\n" +
                "} \n" +
                "ORDER BY ?tName \n";

        Query query = QueryFactory.create(treeQuery);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, meshModel)) {
            ResultSet results = qexec.execSelect();
            for (; results.hasNext();) {
                QuerySolution soln = results.nextSolution();
                Resource meshTop = soln.getResource("d");
                Resource prefConcept = meshTop.getProperty(this.meshvPreferredConcept).getObject().asResource();
                Resource metaTopConcept = this.addMeshConcept(prefConcept);
                Literal tree = soln.getLiteral("tName");
                this.treeLookup.put(tree.getLexicalForm(), metaTopConcept);
                String c_id = metaTopConcept.getURI().replaceAll(META_URI,"");
                String cs_id = c_id.replaceAll("concept_",  "scheme_");
                Resource metaConceptSchema = this.bm.getResource(cs_id);
                metaConceptSchema.addProperty(RDF.type, NamedIndividual);
                metaConceptSchema.addProperty(RDF.type, this.skosConceptScheme);
                metaConceptSchema.addLiteral(label, meshTop.getProperty(label).getLiteral());
                metaConceptSchema.addProperty(this.skosHasTopConcept, metaTopConcept);
                metaTopConcept.addProperty(this.skosInScheme, metaConceptSchema);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //
        // Query over TopicDescriptors in MeSH
        //
        String queryString = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>\n" +
                "PREFIX mesh: <http://id.nlm.nih.gov/mesh/>\n" +
                "PREFIX mesh2020: <http://id.nlm.nih.gov/mesh/2020/>\n" +
                "PREFIX mesh2019: <http://id.nlm.nih.gov/mesh/2019/>\n" +
                "PREFIX mesh2018: <http://id.nlm.nih.gov/mesh/2018/>\n" +
                "SELECT DISTINCT ?d ?dName\n" +
                "WHERE {\n" +
                "  ?d a meshv:TopicalDescriptor .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "} " +
                "ORDER BY ?d";

        //
        // add core of new meta concepts + relationships to ConceptScheme / TopConcept
        //
        query = QueryFactory.create(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(query, meshModel);
        ResultSet results = qexec.execSelect();
        for (; results.hasNext();) {
            QuerySolution soln = results.nextSolution();
            Resource meshDesc = soln.getResource("d");
            Resource metaConcept = this.meshDescriptorToSkosConcept(meshDesc);
        }

    }

    // ALL: 'C_ID', 'C_NAME', 'CUI', 'ONT_NAME', 'ATOMS', 'SRC_ID'
    // NEW: 'CUI', 'ONT_NAME', 'NEW_SRC_ID', 'ATOMS'
    // MERGED: 'C_ID', 'C_NAME', 'RETIRED_CUI', 'ONT_NAME', 'NEW_CUI', 'NEW_SRC_ID', 'MAPPING', 'ATOMS''C_ID', 'C_NAME', 'CUI', 'ONT_NAME', 'ATOMS', 'SRC_ID'
    // DELETED: 'C_ID', 'C_NAME', 'RETIRED_CUI', 'MAPPING'
    private void readLookup(File conceptsDir) throws FileNotFoundException {

        File all_conceptsTsv = new File(conceptsDir.getPath() + "/meta-kg-dump-mesh2020.tsv");
        File merged_conceptsTsv = new File(conceptsDir.getPath() + "/meta-kg-dump-mesh2020-merged_concepts.tsv");
        File new_conceptsTsv  = new File(conceptsDir.getPath() + "/meta-kg-dump-mesh2020-new_concepts.tsv");
        File deleted_conceptsTsv = new File(conceptsDir.getPath() + "/meta-kg-dump-mesh2020-deleted_concepts.tsv");

        this.records = new ArrayList<UmlsMeshMetaRecord>();
        this.meshRecords = new HashMap<String, UmlsMeshMetaRecord>();
        this.metaRecords = new HashMap<String, UmlsMeshMetaRecord>();
        this.umlsRecords = new HashMap<String, UmlsMeshMetaRecord>();

        TsvParserSettings settings = new TsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        TsvParser p = new TsvParser(settings);
        List<List<String[]>> tsv_data = new ArrayList<List<String[]>>();
        tsv_data.add(p.parseAll(new FileReader(all_conceptsTsv.getPath()))); // all
        p = new TsvParser(settings);
        tsv_data.add(p.parseAll(new FileReader(merged_conceptsTsv.getPath()))); // merged
        p = new TsvParser(settings);
        tsv_data.add(p.parseAll(new FileReader(new_conceptsTsv.getPath()))); // new
        p = new TsvParser(settings);
        tsv_data.add(p.parseAll(new FileReader(deleted_conceptsTsv.getPath()))); // deleted

        for( List<String[]> data : tsv_data ){
            String[] h = data.get(0);
            for( int i=1;  i<data.size(); i++) {
                String[] row = data.get(i);
                Map<String, String> l = new HashMap();
                for (int j = 0; j < h.length; j++) {
                    l.put(h[j], row[j]);
                }
                String meta = "";
                if (l.containsKey("C_ID"))
                    meta = l.get("C_ID");
                String mesh = "";
                if (l.containsKey("SRC_ID"))
                    mesh = l.get("SRC_ID");
                String umls = "";
                if (l.containsKey("CUI"))
                    umls = l.get("CUI");
                UmlsMeshMetaRecord mmu = new UmlsMeshMetaRecord(meta, mesh, umls);
                this.records.add(new UmlsMeshMetaRecord(meta, mesh, umls));
                if(mesh.length() > 0)
                    this.meshRecords.put(mesh, mmu);
                if(meta.length() > 0)
                    this.metaRecords.put(meta, mmu);
                if(umls.length() > 0)
                    this.umlsRecords.put(umls, mmu);
            }
        }
    }

    public void writeToOut(File outFile) throws IOException {
        outFile.delete();
        PrintWriter out;
        out = new PrintWriter(new BufferedWriter(
                new FileWriter(outFile, true)));
        this.bm.write(out, "TTL");
        out.close();
    }

    public void run(File conceptsDir, File meshFile, File outFile) throws Exception {
        this.readLookup(conceptsDir);
        this.buildSkosFromMesh(meshFile);
        this.writeToOut(outFile);
    }

    public static class Options {

        @Option(name = "-meshFile", usage = "Input Mesh File", required = true, metaVar = "MESH-FILE")
        public File meshFile;

        @Option(name = "-conceptsDir", usage = "Concepts TSV Directory", required = true, metaVar = "CONCEPTS-DIR")
        public File conceptsDir;

        @Option(name = "-skosFile", usage = "Output Meta-SKOS File", required = true, metaVar = "SKOS-FILE")
        public File skosFile;

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

        AddMondoToSkos m2s = new AddMondoToSkos();
        m2s.run(options.conceptsDir, options.meshFile, options.skosFile);

    }
}
