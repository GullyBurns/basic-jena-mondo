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


public class MeshToSkos {

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

    public MeshToSkos() throws IOException {

        // create the base model
        this.bm = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF );

        for( String l : meshCategoriesString.split("\n") ) {
            String[] ll = l.split("\t");
            this.meshCategories.put(ll[0],ll[1]);
        }

        // create the reasoning model using the base
        this.m = ModelFactory.createOntologyModel( OntModelSpec.OWL_DL_MEM_TRANS_INF, this.bm );
        this.dm = this.bm.getDocumentManager();

        ClassLoader classLoader = MeshToSkos.class.getClassLoader();

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

    private String getMetaUriFromMesh(Resource mesh) {

        if( this.master_meta_lookup.containsKey(mesh.getURI()) ){
            return this.master_meta_lookup.get(mesh.getURI());
        }

        String curi = mesh.getURI().replaceAll(MESH_URI, "");
        String c_id = META_URI+"concept_"+this.max_c_id;
        if(this.meshRecords.containsKey(curi) &&
                this.meshRecords.get(curi).meta.length() > 0) {
            c_id = META_URI+"concept_" + this.meshRecords.get(curi).meta;
        } else {
            this.max_c_id++;
        }
        this.master_meta_lookup.put(mesh.getURI(), c_id);

        return c_id;

    }

    private boolean addMetaConceptFromMondo(Resource r){
        String c_id = this.getMetaUriFromMondo(r);
        Resource metaConcept = this.bm.getResource(c_id);
        if(!metaConcept.hasProperty(label)) {
            this.addMondoConcept(r);
            return true;
        } else {
            metaConcept.addProperty(this.skosExactMatch, r);
        }
        return false;
    }

    private String getMetaUriFromMondo(Resource mondo) {

        if( this.master_meta_lookup.containsKey(mondo.getURI()) ){
            return this.master_meta_lookup.get(mondo.getURI());
        }

        String xrefC = this.getMeshCode(mondo);
        if( xrefC != null && this.master_meta_lookup.containsKey(MESH_URI + xrefC)) {
            String metaURI = this.master_meta_lookup.get(MESH_URI + xrefC);
            this.master_meta_lookup.put(mondo.getURI(), metaURI);
            return metaURI;
        }

        String c_id = META_URI+"concept_"+this.max_c_id;
        this.max_c_id++;

        this.master_meta_lookup.put(mondo.getURI(), c_id);

        return c_id;

    }

    private Resource meshDescriptorToSkosConcept(Resource meshDesc) throws Exception {

        Resource prefConcept = meshDesc.getProperty(this.meshvPreferredConcept).getObject().asResource();
        Resource conceptScheme = this.getConceptScheme(meshDesc);

        Resource metaConcept = this.addMeshConcept(prefConcept, conceptScheme);
        this.master_meta_lookup.put(meshDesc.getURI(), metaConcept.getURI());

        this.addExactMatches(metaConcept, meshDesc);

        this.addNarrowerConcepts(metaConcept, prefConcept, conceptScheme);

        StmtIterator nIt = prefConcept.listProperties(m.getProperty(MESHV_URI + "broaderConcept"));
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

            Resource meshParentDesc = s.getObject().as(Resource.class);
            Resource meshParent = meshParentDesc.getProperty(this.meshvPreferredConcept).getObject().asResource();
            Resource metaParent = this.addMeshConcept(meshParent, conceptScheme);
            this.master_meta_lookup.put(meshParentDesc.getURI(), metaParent.getURI());

            this.bm.add(metaConcept, this.skosBroader, metaParent);
            this.bm.add(metaParent, this.skosNarrower, metaConcept);
            if( !meshParent.getProperty(label).getLiteral().getLexicalForm().equals(
                    metaParent.getProperty(label).getLiteral().getLexicalForm() ) ) {
                this.writer.write("\t\t" + meshParent.getProperty(label).getLiteral().getLexicalForm() + " (" +
                        meshParent.getURI().replaceAll(MESH_URI, "") + ")\tnarrower\t" +
                        meshDesc.getProperty(label).getLiteral().getLexicalForm() + " (" +
                        meshDesc.getURI().replaceAll(MESH_URI, "") + ")\t\t");
                this.writer.write("\t\t" + metaParent.getProperty(label).getLiteral().getLexicalForm() + " (" +
                        metaParent.getURI().replaceAll(META_URI, "") + ")\tnarrower\t" +
                        metaConcept.getProperty(label).getLiteral().getLexicalForm() + " (" +
                        metaConcept.getURI().replaceAll(META_URI, "") + ")\n");
                this.writer.close();
                throw new Exception("Mismatch between mesh / meta");
            }
        }

        /*pIt = meshDesc.listProperties(this.meshvNarrower);
        while(pIt.hasNext()){
            Statement s = pIt.nextStatement();
            Resource meshChild = s.getObject().as(Resource.class);
            Resource metaChild = this.addMeshConcept(meshChild, conceptScheme);
            this.bm.add(metaConcept, this.skosNarrower, metaChild);
            this.bm.add(metaChild, this.skosBroader, metaConcept);
            this.writer.write("\t\t"+meshChild.getProperty(label).getLiteral().getLexicalForm()+" ("+
                    meshChild.getURI().replaceAll(MESH_URI,"")+")\tbroader\t"+
                    meshDesc.getProperty(label).getLiteral().getLexicalForm()+" ("+
                    meshDesc.getURI().replaceAll(MESH_URI,"")+")\t\t");
            this.writer.write("\t\t"+metaChild.getProperty(label).getLiteral().getLexicalForm()+" ("+
                    metaChild.getURI().replaceAll(MESH_URI,"")+")\tbroader\t"+
                    metaConcept.getProperty(label).getLiteral().getLexicalForm()+" ("+
                    metaConcept.getURI().replaceAll(MESH_URI,"")+")\n");
        }*/

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
        StmtIterator nIt = meshConcept.listProperties(m.getProperty(MESHV_URI + "narrowerConcept"));
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

    void writeDebugComparison(Resource mesh, Resource meta) throws IOException {
        this.writer.write("\t\t" + mesh.getProperty(label).getLiteral().getLexicalForm() + " (" +
                mesh.getURI().replaceAll(MESH_URI, "") + ") -->");
        this.writer.write("\t\t" + meta.getProperty(label).getLiteral().getLexicalForm() + " (" +
                meta.getURI().replaceAll(META_URI, "") + ")\n");
    }

    Resource addMeshConcept(Resource meshConcept) {
        String c_id = getMetaUriFromMesh(meshConcept);

        Resource metaConcept = this.bm.getResource(c_id);

        // return if details for this concept already filled in.
        if( metaConcept.hasProperty(label) ) {
            try {
                this.writeDebugComparison(meshConcept, metaConcept);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return metaConcept;
        }

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

        try {
            this.writeDebugComparison(meshConcept, metaConcept);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return metaConcept;

    }

    Resource addMondoConcept(Resource mondoConcept) {
        String c_id = this.getMetaUriFromMondo(mondoConcept);
        Resource metaConcept = this.bm.getResource(c_id);

        // return if details for this concept already filled in.
        if( metaConcept.hasProperty(label) )
            return metaConcept;

        this.bm.add(metaConcept, this.skosExactMatch, mondoConcept);
        // TODO: Need to import equivalences from MONDO here as well.

        metaConcept.addProperty(RDF.type, NamedIndividual);
        metaConcept.addProperty(RDF.type, this.skosConcept);
        if( mondoConcept.getProperty(label) != null ) {
            metaConcept.addLiteral(label, mondoConcept.getProperty(label).getLiteral());
        } else {
            int debug=1;
        }
        if( mondoConcept.getProperty(this.iaoDefinition) != null ) {
            Literal iaoDef = mondoConcept.getProperty(this.iaoDefinition).getLiteral();
            metaConcept.addLiteral(this.skosDefiniton, iaoDef);
        }

        Set<Literal> altLabels = new HashSet<Literal>();
        StmtIterator tIt = mondoConcept.listProperties(m.getProperty(OBOINOWL_URI + "hasExactSynonym"));
        while(tIt.hasNext()) {
            Statement s2 = tIt.nextStatement();
            altLabels.add(s2.getObject().asLiteral());
        }
        for(Literal l : altLabels){
            this.bm.addLiteral(metaConcept, this.skosAltLabel, l);
        }

        return metaConcept;

    }

    public void buildSkosFromMesh(File meshFile) throws Exception {

        Model mesh_schema = FileManager.get().loadModel(MESHV_URI);
        Model data = FileManager.get().loadModel(meshFile.getPath());
        Reasoner reasoner = ReasonerRegistry.getRDFSReasoner();
        reasoner = reasoner.bindSchema(mesh_schema);
        InfModel meshModel = ModelFactory.createInfModel(reasoner, data);

        this.meshvBroaderDescriptor = meshModel.getProperty(MESHV_URI +"broaderDescriptor");
        this.meshvNarrowerDescriptor = meshModel.getProperty(MESHV_URI +"narrowerDescriptor");
        this.meshvBroader = meshModel.getProperty(MESHV_URI +"broader");
        this.meshvNarrower = meshModel.getProperty(MESHV_URI +"narrower");
        this.meshvScopeNote = meshModel.getProperty(MESHV_URI +"scopeNote");
        this.meshvPreferredConcept = meshModel.getProperty(MESHV_URI +"preferredConcept");
        this.meshvConcept = meshModel.getProperty(MESHV_URI +"concept");
        this.meshvTerm = meshModel.getProperty(MESHV_URI +"term");
        this.meshTreeNumber = meshModel.getProperty(MESHV_URI +"treeNumber");

        for (String code : this.meshCategories.keySet() ) {
            Resource metaConceptSchema = this.bm.getResource(META_URI+"schema_"+code);
            metaConceptSchema.addProperty(RDF.type, NamedIndividual);
            metaConceptSchema.addProperty(RDF.type, this.skosConceptScheme);
            metaConceptSchema.addLiteral(label, this.meshCategories.get(code));
        }

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
                "  FILTER( lang(?dName) = 'en')\n" +
                "  FILTER( regex(?tName, \"^[A-Z][0-9]+$\") )\n" +
                "} \n" +
                "ORDER BY ?tName \n";

        Query query = QueryFactory.create(treeQuery);
        QueryExecution qexec = QueryExecutionFactory.create(query, meshModel);
        ResultSet results = qexec.execSelect();
        for (; results.hasNext();) {
            QuerySolution soln = results.nextSolution();
            Resource meshTop = soln.getResource("d");
            Resource prefConcept = meshTop.getProperty(this.meshvPreferredConcept).getObject().asResource();
            Resource metaTopConcept = this.addMeshConcept(prefConcept);
            Literal tree = soln.getLiteral("tName");
            this.treeLookup.put(tree.getLexicalForm(), metaTopConcept);
            String cs_id = metaTopConcept.getURI().replaceAll("concept_",  "scheme_");
            Resource metaConceptSchema = this.bm.getResource(cs_id);
            metaConceptSchema.addProperty(RDF.type, NamedIndividual);
            metaConceptSchema.addProperty(RDF.type, this.skosConceptScheme);
            metaConceptSchema.addLiteral(label, meshTop.getProperty(label).getLiteral());
            metaConceptSchema.addProperty(this.skosHasTopConcept, metaTopConcept);
            metaTopConcept.addProperty(this.skosInScheme, metaConceptSchema);
            Resource categoryConceptScheme = this.bm.getResource(META_URI+tree.getLexicalForm().substring(0,1));
            metaTopConcept.addProperty(this.skosInScheme, categoryConceptScheme);
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
                "  FILTER( lang(?dName) = 'en')\n" +
                "} " +
                "ORDER BY ?d";

        //
        // add core of new meta concepts + relationships to ConceptScheme / TopConcept
        //
        query = QueryFactory.create(queryString);
        qexec = QueryExecutionFactory.create(query, meshModel);
        results = qexec.execSelect();
        writer.write("DESCRIPTORS\n");
        for (; results.hasNext();) {
            QuerySolution soln = results.nextSolution();
            Resource meshDesc = soln.getResource("d");
            Literal meshName = soln.getLiteral("dName");
            writer.write("\t"+meshDesc.getURI().replaceAll(MESH_URI,"") + "\t" + meshName.getLexicalForm() + "\n");
            Resource metaConcept = this.meshDescriptorToSkosConcept(meshDesc);
            metaConcept.addProperty(label, meshName);
            int i = 0;
        }

        writer.close();


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

    public void addMondo(File mondoFile) throws FileNotFoundException {

        Model mondoModel = ModelFactory.createDefaultModel();
        mondoModel.read(new FileInputStream(mondoFile), null, "TTL");

        String allDiseaseQuery = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?leaf ?midJ ?midI (count(?counter) as ?position) ?d  \n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d skos:exactMatch ?xref .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  ?counter rdfs:subClassOf* ?d .\n" +
                "  ?midI rdfs:subClassOf* ?counter .\n" +
                "  ?midJ rdfs:subClassOf ?midI .\n" +
                "  ?leaf rdfs:subClassOf* ?midJ .\n" +
                "  FILTER( regex(str(?xref), \"(mesh)\") )\n" +
                "  FILTER NOT EXISTS { [] rdfs:subClassOf ?leaf }\n" +
                "} \n" +
                "GROUP BY ?d ?midJ ?midI ?leaf \n" +
                "ORDER BY ?leaf ?d DESC(?position) ?midI \n";

        Query query = QueryFactory.create(allDiseaseQuery);
        QueryExecution qexec = QueryExecutionFactory.create(query, mondoModel);
        ResultSet results = qexec.execSelect();

        List<PathElement> peList = new ArrayList<PathElement>();
        for (; results.hasNext();) {
            QuerySolution soln = results.nextSolution();
            Resource d = soln.getResource("d");
            Resource midJ = soln.getResource("midJ");
            Resource midI = soln.getResource("midI");
            Resource leaf = soln.getResource("leaf");
            Literal c = soln.getLiteral("position");
            PathElement pe = new PathElement(d, midI, midJ, leaf);
            if(peList.size() == 0 || peList.get(peList.size()-1) != pe) {
                peList.add(new PathElement(d, midI, midJ, leaf));
            }
        }

        //
        // Basic idea is that the sparql query above provides a table of anchor -> leaf pathways
        // where the MONDO anchors have MeSH terms.
        // We will trace back from the leaves up the hierarchy until we hit a MONDO node with skos:exactMatch
        // to a MeSH term at which point we will jump to to the next anchor -> leaf pathway.
        // Note that the terminating node may not be the anchor and there are a lot of repetitions.
        //
        Resource current_meta_leaf = null;
        Resource current_mondo_leaf = null;
        Resource current_mondo_anchor = null;
        Resource current_meta_anchor = null;
        List<PathElement> current_path = new ArrayList<PathElement>();
        boolean go = true;
        for(PathElement pe : peList) {
            if( pe.anchor != current_mondo_anchor || pe.leaf != current_mondo_leaf ) {
                current_mondo_leaf = pe.leaf;
                current_mondo_anchor = pe.anchor;
                Resource meshXref = this.bm.getResource(MESH_URI + this.getMeshCode(current_mondo_anchor) );
                if( this.master_meta_lookup.containsKey(meshXref.getURI()) ) {
                    current_meta_leaf = this.addMondoConcept(pe.leaf);
                    current_meta_anchor = this.bm.getResource(this.master_meta_lookup.get(meshXref.getURI()));
                    go = true;
                } else {
                    go = false;
                }
            }

            if( go ) {

                // check to see if pe.p1 already exists in metaskos. If so, add the elements of current_path
                // and  jump to the next anchor->leaf pathway
                String c_id = this.getMetaUriFromMondo(pe.p1);
                Resource metaConcept = this.bm.getResource(c_id);
                if (metaConcept.hasProperty(label)) {
                    Resource first_step = null;
                    for(PathElement to_add : current_path) {
                        Resource m1 = this.addMondoConcept(to_add.p1);
                        Resource m2 = this.addMondoConcept(to_add.p2);
                        this.bm.add(m1, this.skosNarrower, m2);
                        this.bm.add(m2, this.skosBroader, m1);
                        first_step = m1;
                    }
                    if( first_step != null ) {
                        this.bm.add(current_meta_anchor, this.skosBroader, first_step);
                        this.bm.add(first_step, this.skosNarrower, current_meta_anchor);
                    }
                    current_path = new ArrayList<PathElement>();
                    go = false;
                    continue;  // skip the rest of the loop.
                }

                // add to path
                current_path.add(pe);
                String meshXref = this.getMeshCode(pe.p1);
                if(meshXref != null ) {
                    Resource first_step = null;
                    for(PathElement to_add : current_path) {
                        Resource m1 = this.addMondoConcept(to_add.p1);
                        Resource m2 = this.addMondoConcept(to_add.p2);
                        this.bm.add(m1, this.skosNarrower, m2);
                        this.bm.add(m2, this.skosBroader, m1);
                        first_step = m1;
                    }
                    if( first_step != null ) {
                        this.bm.add(current_meta_anchor, this.skosNarrower, first_step);
                        this.bm.add(first_step, this.skosBroader, current_meta_anchor);
                    }
                    current_path = new ArrayList<PathElement>();
                    go = false;
                }
            }
        }
    }


    public void addMondo2(File mondoFile) throws FileNotFoundException {

        Model mondoModel = ModelFactory.createDefaultModel();
        mondoModel.read(new FileInputStream(mondoFile), null, "TTL");

        String allDiseaseQuery = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX obo: <http://purl.obolibrary.org/obo/>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "SELECT DISTINCT ?d ?dName\n" +
                "WHERE {\n" +
                "  ?d rdf:type owl:Class .\n" +
                "  ?d rdfs:label ?dName .\n" +
                "  ?d rdfs:subClassOf+ obo:MONDO_0000001 .\n" +
                "  FILTER NOT EXISTS { [] rdfs:subClassOf ?d }\n" +
                "} \n";

        Query query = QueryFactory.create(allDiseaseQuery);
        QueryExecution qexec = QueryExecutionFactory.create(query, mondoModel);
        ResultSet results = qexec.execSelect();
        Set<Resource> added = new HashSet<Resource>();

        //
        // Given a leaf, trace it's subClassOf hierarchy back up the hierarchy until it hits a MeSH-encoded node in
        // the hierarchy and  link it up  and then move to the next leaf. If a node has already been added, move to
        // the next leaf. Should behave like a depth first search.
        //
        for (; results.hasNext();) {
            QuerySolution soln = results.nextSolution();
            Resource current_mondo_leaf = soln.getResource("d");

            if( addMetaConceptFromMondo(current_mondo_leaf) )
                added.add(current_mondo_leaf);

            Set<Resource> all_parents = this.getAllParents(current_mondo_leaf,new HashSet<Resource>());
            for(Resource r : all_parents) {
                if( added.contains(r) )
                    continue;
                if( addMetaConceptFromMondo(r) )
                    added.add(current_mondo_leaf);
            }

            Set<Statement> all_subClassStatements = this.getAllParentStatements(current_mondo_leaf,new HashSet<Statement>());
            for(Statement s : all_subClassStatements) {
                Resource child_mondo = s.getSubject().asResource();
                Resource parent_mondo = s.getObject().asResource();
                String child_id = this.getMetaUriFromMondo(child_mondo);
                Resource child_meta = this.bm.getResource(child_id);
                String parent_id = this.getMetaUriFromMondo(parent_mondo);
                Resource parent_meta = this.bm.getResource(parent_id);
                if(!child_meta.hasProperty(this.skosBroader, parent_meta)) {
                    child_meta.addProperty(this.skosBroader, parent_meta);
                }
                if(!parent_meta.hasProperty(this.skosNarrower, child_meta)) {
                    parent_meta.addProperty(this.skosNarrower, child_meta);
                }
            }

        }
    }

    private Set<Resource> getAllParents(Resource r, Set<Resource> parents) {
        StmtIterator scIt = r.listProperties(subClassOf);
        while (scIt.hasNext()) {
            Statement s = scIt.nextStatement();
            Resource parent = s.getObject().as(Resource.class);
            if (parent.getURI() != null) {
                parents.add(parent);

                // Check if the parent (in MONDO) has a MeSH code.
                // If it does, add this parent but don't propagate above it.
                Resource meshXref = this.bm.getResource(MESH_URI + this.getMeshCode(parent) );
                if( !this.master_meta_lookup.containsKey(meshXref.getURI()) ) {
                    parents = this.getAllParents(parent, parents);
                } else {
                    int pause = 0;
                }
            }
        }
        return parents;
    }

    private Set<Statement> getAllParentStatements(Resource r, Set<Statement> statements) {
        StmtIterator scIt = r.listProperties(subClassOf);
        while (scIt.hasNext()) {
            Statement s2 = scIt.nextStatement();
            Resource parent = s2.getObject().as(Resource.class);
            if (parent.getURI() != null) {
                statements.add(s2);

                // Check if the parent (in MONDO) has a MeSH code.
                // If it does, add this parent but don't propagate above it.
                Resource meshXref = this.bm.getResource(MESH_URI + this.getMeshCode(parent) );
                if( !this.master_meta_lookup.containsKey(meshXref.getURI()) ) {
                    statements = this.getAllParentStatements(parent, statements);
                }

            }
        }
        return statements;
    }

    private String getMeshCode(Resource r) {
        StmtIterator nIt = r.listProperties(this.skosExactMatch);
        while (nIt.hasNext()) {
            Statement s = nIt.nextStatement();
            Resource xrefC = s.getObject().as(Resource.class);
            if (xrefC.getURI().contains("mesh")) {
                return xrefC.getLocalName();
            }
        }
        return null;
    }

    public void writeToOut(File outFile) throws IOException {
        outFile.delete();
        PrintWriter out;
        out = new PrintWriter(new BufferedWriter(
                new FileWriter(outFile, true)));
        this.bm.write(out, "TTL");
        out.close();
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

        MeshToSkos m2s = new MeshToSkos();
        File f = new File(options.meshFile.getParent() + "/log1.txt");
        if( f.exists())
            f.delete();
        m2s.writer = new BufferedWriter(new FileWriter(f));

        m2s.readLookup(options.conceptsDir);

        if( options.skosFile.exists() ) {
            m2s.bm.read(new FileInputStream(options.skosFile), null, "TTL");
            String metaConceptCount =
                    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                            "SELECT (count(?d) as ?c) \n" +
                            "WHERE {\n" +
                            "  ?d rdf:type skos:Concept .\n" +
                            "} \n";
            Query query = QueryFactory.create(metaConceptCount);
            QueryExecution qexec = QueryExecutionFactory.create(query, m2s.bm);
            ResultSet results = qexec.execSelect();
            QuerySolution soln = results.nextSolution();
            Literal c = soln.getLiteral("c");
            m2s.max_c_id = c.getInt()+1000;
            String sparqlReadMetaMeshLooup =
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                    "SELECT DISTINCT ?d ?xref \n" +
                    "WHERE {\n" +
                    "  ?d rdf:type skos:Concept .\n" +
                    "  ?d skos:exactMatch ?xref .\n" +
                    "  FILTER( regex(str(?xref), \"(mesh)\") )\n" +
                    "} \n";
            Query query1 = QueryFactory.create(sparqlReadMetaMeshLooup);
            QueryExecution qexec1 = QueryExecutionFactory.create(query1, m2s.bm);
            ResultSet results1 = qexec1.execSelect();
            for (; results1.hasNext(); ) {
                QuerySolution soln1 = results1.nextSolution();
                Resource d = soln1.getResource("d");
                Resource x = soln1.getResource("xref");
                m2s.master_meta_lookup.put(x.getURI(), d.getURI());
            }
        } else {
            m2s.buildSkosFromMesh(options.meshFile);
        }

        //m2s.countMondoXrefs(options.mondoFile);
        m2s.addMondo2(options.mondoFile);
        options.skosFile.delete();
        m2s.writeToOut(new File(options.skosFile.getPath()) );

    }
}
