package gov.va.rxnorm;

import gov.va.oia.terminology.converters.sharedUtils.ConsoleUtil;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_Descriptions;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_MemberRefsets;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.Property;
import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.ValuePropertyPair;
import gov.va.oia.terminology.converters.sharedUtils.stats.ConverterUUID;
import gov.va.oia.terminology.converters.umlsUtils.RRFBaseConverterMojo;
import gov.va.oia.terminology.converters.umlsUtils.RRFDatabaseHandle;
import gov.va.oia.terminology.converters.umlsUtils.UMLSFileReader;
import gov.va.oia.terminology.converters.umlsUtils.ValuePropertyPairWithAttributes;
import gov.va.oia.terminology.converters.umlsUtils.rrf.REL;
import gov.va.oia.terminology.converters.umlsUtils.sql.TableDefinition;
import gov.va.rxnorm.propertyTypes.PT_Annotations;
import gov.va.rxnorm.rrf.RXNCONSO;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.ihtsdo.otf.tcc.api.coordinate.Status;
import org.ihtsdo.otf.tcc.api.metadata.binding.Snomed;
import org.ihtsdo.otf.tcc.dto.TtkConceptChronicle;
import org.ihtsdo.otf.tcc.dto.component.TtkComponentChronicle;
import org.ihtsdo.otf.tcc.dto.component.description.TtkDescriptionChronicle;
import org.ihtsdo.otf.tcc.dto.component.refexDynamic.TtkRefexDynamicMemberChronicle;
import org.ihtsdo.otf.tcc.dto.component.refexDynamic.data.dataTypes.TtkRefexDynamicString;
import org.ihtsdo.otf.tcc.dto.component.relationship.TtkRelationshipChronicle;

/**
 * Loader code to convert RxNorm into the workbench.
 */
@Mojo( name = "convert-RxNormH-to-jbin", defaultPhase = LifecyclePhase.PROCESS_SOURCES )
public class RxNormMojo extends RRFBaseConverterMojo
{
	private TtkConceptChronicle allCUIRefsetConcept_;
	private TtkConceptChronicle cpcRefsetConcept_;
	public static final String cpcRefsetConceptKey_ = "Current Prescribable Content";
	
	private PreparedStatement semanticTypeStatement, conSat, cuiRelStatementForward, cuiRelStatementBackward;
	
	private UUID rxNormIngredients;
	private HashSet<String> allowedCUIs;
	AtomicInteger skippedRelForNotMatchingCUIFilter = new AtomicInteger();
	
	@Override
	public void execute() throws MojoExecutionException
	{
		try
		{
			outputDirectory.mkdir();

			String fileNameDatePortion = loadDatabase();
			SimpleDateFormat sdf = new SimpleDateFormat("MMddYYYY");
			long defaultTime = sdf.parse(fileNameDatePortion).getTime();
			
			init(outputDirectory, "RxNormH", "RXN", new PT_Annotations(), Arrays.asList(new String[] {"RXNORM"}), null,
					Arrays.asList(new String[] {"has_ingredient"}), defaultTime);
			
			allCUIRefsetConcept_ = ptUMLSRefsets_.getConcept(ptUMLSRefsets_.CUI_CONCEPTS.getSourcePropertyNameFSN());
			
			cpcRefsetConcept_ = ptRefsets_.get("RXNORM").getConcept(cpcRefsetConceptKey_);
			
			//special rule - create an ingredients concept
			TtkConceptChronicle ingredients = eConcepts_.createConcept("RxNorm Ingredients", Snomed.PRODUCT.getUuids()[0]);
			rxNormIngredients = ingredients.getPrimordialUuid();
			
			ingredients.writeExternal(dos_);

			// Add version data to allRefsetConcept
			eConcepts_.addStringAnnotation(allCUIRefsetConcept_, loaderVersion,  ptContentVersion_.LOADER_VERSION.getUUID(), Status.ACTIVE);
			eConcepts_.addStringAnnotation(allCUIRefsetConcept_, converterResultVersion, ptContentVersion_.RELEASE.getUUID(), Status.ACTIVE);
			
			semanticTypeStatement = db_.getConnection().prepareStatement("select TUI, ATUI, CVF from RXNSTY where RXCUI = ?");
			conSat = db_.getConnection().prepareStatement("select * from RXNSAT where RXCUI = ? and RXAUI = ? and (SAB='RXNORM' or ATN='NDC')");

			//UMLS and RXNORM do different things with rels - UMLS never has null CUI's, while RxNorm always has null CUI's (when AUI is specified)
			//Also need to join back to MRCONSO to make sure that the target concept is one that we will load with the SAB filter in place.
			cuiRelStatementForward = db_.getConnection().prepareStatement("SELECT r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from RXNREL as r, RXNCONSO "
					+ "WHERE RXCUI2 = ? and RXAUI2 is null and r.SAB='RXNORM' and r.RXCUI1 = RXNCONSO.RXCUI and RXNCONSO.SAB='RXNORM'");

			cuiRelStatementBackward = db_.getConnection().prepareStatement("SELECT r.RXCUI1, r.RXAUI1, r.STYPE1, r.REL, r.RXCUI2, r.RXAUI2, r.STYPE2, "
					+ "r.RELA, r.RUI, r.SRUI, r.SAB, r.SL, r.DIR, r.RG, r.SUPPRESS, r.CVF from RXNREL as r, RXNCONSO "
					+ "WHERE RXCUI1 = ? and RXAUI1 is null and r.SAB='RXNORM' and r.RXCUI2 = RXNCONSO.RXCUI and RXNCONSO.SAB='RXNORM'");
			
			//Set this to true if you are trying to load on a limited-memory system...
			//ConverterUUID.disableUUIDMap_ = false;
			int cuiCounter = 0;

			Statement statement = db_.getConnection().createStatement();
			
			
			//Rule from John - only allow TTY = IN and TTY = SCDC - need to preprocess, to find every RXCUI that we will end up creating as a concept
			//so we can pass in this 'valid' list of concepts to the relationship gen code, so we don't generate rels outside of these concepts
			allowedCUIs = new HashSet<>();
			ResultSet rs = statement.executeQuery("select RXCUI from RXNCONSO where SAB='RXNORM' and (TTY = 'IN' or TTY = 'SCDC')");
			while (rs.next())
			{
				allowedCUIs.add(rs.getString("RXCUI"));
			}
			rs.close();
			
			rs = statement.executeQuery("select RXCUI, LAT, RXAUI, SAUI, SCUI, SAB, TTY, CODE, STR, SUPPRESS, CVF from RXNCONSO " 
					+ "where SAB='RXNORM' order by RXCUI" );
			
			HashSet<String> skippedCUIForNotMatchingCUIFilter = new HashSet<String>();
			
			ArrayList<RXNCONSO> conceptData = new ArrayList<>();
			while (rs.next())
			{
				RXNCONSO current = new RXNCONSO(rs);
				if (!allowedCUIs.contains(current.rxcui))
				{
					skippedCUIForNotMatchingCUIFilter.add(current.rxcui);
					continue;
				}
				if (conceptData.size() > 0 && !conceptData.get(0).rxcui.equals(current.rxcui))
				{
					processCUIRows(conceptData);
					if (cuiCounter % 100 == 0)
					{
						ConsoleUtil.showProgress();
					}
					cuiCounter++;
					if (cuiCounter % 10000 == 0)
					{
						ConsoleUtil.println("Processed " + cuiCounter + " CUIs creating " + eConcepts_.getLoadStats().getConceptCount() + " concepts");
					}
					conceptData.clear();
				}
				
				conceptData.add(current);
			}
			rs.close();
			statement.close();

			// process last
			processCUIRows(conceptData);
			
			ConsoleUtil.println("Processed " + cuiCounter + " CUIs creating " + eConcepts_.getLoadStats().getConceptCount() + " concepts");
			ConsoleUtil.println("Skipped " + skippedCUIForNotMatchingCUIFilter.size() + " concepts for not containing the desired TTY");
			ConsoleUtil.println("Skipped " + skippedRelForNotMatchingCUIFilter + " relationships for linking to a concept we didn't include");

			semanticTypeStatement.close();
			conSat.close();
			cuiRelStatementForward.close();
			cuiRelStatementBackward.close();
			finish(outputDirectory);
			
			db_.shutdown();
		}
		catch (Exception e)
		{
			throw new MojoExecutionException("Failure during conversion", e);
		}
	}
	
	/**
	 * Returns the date portion of the file name - so from 'RxNorm_full_09022014.zip' it returns 09022014
	 */
	private String loadDatabase() throws Exception
	{
		// Set up the DB for loading the temp data
		String toReturn = null;
		
		// Read the RRF file directly from the source zip file - need to find the zip first, to get the date out of the file name.
		ZipFile zf = null;
		for (File f : inputFileLocation.listFiles())
		{
			if (f.getName().toLowerCase().startsWith("rxnorm_full_") && f.getName().toLowerCase().endsWith(".zip"))
			{
				zf = new ZipFile(f);
				toReturn = f.getName().substring("rxnorm_full_".length());
				toReturn = toReturn.substring(0, toReturn.length() - 4); 
				break;
			}
		}
		if (zf == null)
		{
			throw new MojoExecutionException("Can't find source zip file");
		}
		
		db_ = new RRFDatabaseHandle();
		File dbFile = new File(outputDirectory, "rrfDB.h2.db");
		boolean createdNew = db_.createOrOpenDatabase(new File(outputDirectory, "rrfDB"));

		if (!createdNew)
		{
			ConsoleUtil.println("Using existing database.  To load from scratch, delete the file '" + dbFile.getAbsolutePath() + ".*'");
		}
		else
		{
			// RxNorm doesn't give us the UMLS tables that define the table definitions, so I put them into an XML file.
			List<TableDefinition> tables = db_.loadTableDefinitionsFromXML(RxNormMojo.class.getResourceAsStream("/RxNormTableDefinitions.xml"));

			for (TableDefinition td : tables)
			{
				ZipEntry ze = zf.getEntry("rrf/" + td.getTableName() + ".RRF");
				if (ze == null)
				{
					throw new MojoExecutionException("Can't find the file 'rrf/" + td.getTableName() + ".RRF' in the zip file");
				}

				db_.loadDataIntoTable(td, new UMLSFileReader(new BufferedReader(new InputStreamReader(zf.getInputStream(ze), "UTF-8"))), null);
			}
			zf.close();

			// Build some indexes to support the queries we will run

			Statement s = db_.getConnection().createStatement();
			ConsoleUtil.println("Creating indexes");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_rxcui_index ON RXNCONSO (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX conso_rxaui_index ON RXNCONSO (RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_rxcui_aui_index ON RXNSAT (RXCUI, RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sat_aui_index ON RXNSAT (RXAUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_rxcui_index ON RXNSTY (RXCUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX sty_tui_index ON RXNSTY (TUI)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxcui2_index ON RXNREL (RXCUI2, RXAUI2)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rxaui2_index ON RXNREL (RXCUI1, RXAUI1)");
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_rela_rel_index ON RXNREL (RELA, REL)");  //helps with rel metadata
			ConsoleUtil.showProgress();
			s.execute("CREATE INDEX rel_sab_index ON RXNREL (SAB)");  //helps with rel metadata
			s.close();
		}
		return toReturn;
	}

	private void processCUIRows(ArrayList<RXNCONSO> conceptData) throws IOException, SQLException, PropertyVetoException
	{
		String rxCui = conceptData.get(0).rxcui;
		
		HashSet<String> uniqueTTYs = new HashSet<String>();
		
		
		//ensure all the same CUI, gather the TTYs involved
		for (RXNCONSO row : conceptData)
		{
			uniqueTTYs.add(row.tty);
			if (!row.rxcui.equals(rxCui))
			{
				throw new RuntimeException("Oops");
			}
		}

		TtkConceptChronicle cuiConcept = eConcepts_.createConcept(createCUIConceptUUID(rxCui));
		eConcepts_.addStringAnnotation(cuiConcept, rxCui, ptUMLSAttributes_.getProperty("RXCUI").getUUID(), Status.ACTIVE);
		
		//Special rule from John - if contains a IN TTY type - hang in from our RxNorm Ingredients concept.
		if (uniqueTTYs.contains("IN"))
		{
			eConcepts_.addRelationship(cuiConcept, rxNormIngredients);
		}

		ArrayList<ValuePropertyPairWithSAB> cuiDescriptions = new ArrayList<>();
		HashMap<String, ValuePropertyPairWithSAB> uniqueCodes = new HashMap<>();
		HashSet<String> sabs = new HashSet<>();
		
		for (RXNCONSO atom : conceptData)
		{
			if (!atom.code.equals("NOCODE") && !uniqueCodes.containsKey(atom.code))
			{
				ValuePropertyPairWithSAB code = new ValuePropertyPairWithSAB(atom.code, ptUMLSAttributes_.getProperty("CODE"), atom.sab);
				code.addUUIDAttribute(ptUMLSAttributes_.getProperty("SAB").getUUID(), ptSABs_.getProperty(atom.sab).getUUID());
				uniqueCodes.put(atom.code, code);
			}

			//put it in as a string, so users can search for AUI
			eConcepts_.addStringAnnotation(cuiConcept, atom.rxaui, ptUMLSAttributes_.getProperty("RXAUI").getUUID(), Status.ACTIVE);
				
			ValuePropertyPairWithSAB desc = new ValuePropertyPairWithSAB(atom.str, ptDescriptions_.get(atom.sab).getProperty(atom.tty), atom.sab);
			
			//used for sorting description to figure out what to use for FSN
			cuiDescriptions.add(desc);
			
			desc.addStringAttribute(ptUMLSAttributes_.getProperty("RXAUI").getUUID(), atom.rxaui);
			desc.addUUIDAttribute(ptUMLSAttributes_.getProperty("SAB").getUUID(), ptSABs_.getProperty(atom.sab).getUUID());
				
			if (atom.saui != null)
			{
				desc.addStringAttribute(ptUMLSAttributes_.getProperty("SAUI").getUUID(), atom.saui);
			}
			if (atom.scui != null)
			{
				desc.addStringAttribute(ptUMLSAttributes_.getProperty("SCUI").getUUID(), atom.scui);
			}
				
			if (atom.suppress != null)
			{
				desc.addUUIDAttribute(ptUMLSAttributes_.getProperty("SUPPRESS").getUUID(), ptSuppress_.getProperty(atom.suppress).getUUID());
			}
				
			if (atom.cvf != null)
			{
				if (atom.cvf.equals("4096"))
				{
					desc.addRefsetMembership(cpcRefsetConcept_);
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNCONSO cvf column '" + atom.cvf + "'");
				}
			}
			// T-ODO handle language - not currently a 'live' todo, because RxNorm only has ENG at the moment.
			if (!atom.lat.equals("ENG"))
			{
				ConsoleUtil.printErrorln("Non-english lang settings not handled yet!");
			}
			
			//TODO - at this point, sometime in the future, we make make attributes out of the relationships that occur between the AUIs
			//and store them on the descriptions, since OTF doesn't allow relationships between descriptions

			sabs.add(atom.sab);
		}
		
		for (ValuePropertyPairWithSAB code : uniqueCodes.values())
		{
			eConcepts_.addStringAnnotation(cuiConcept, code.getValue(), code.getProperty().getUUID(), Status.ACTIVE);
		}
		
		for (String sab : sabs)
		{
			eConcepts_.addDynamicRefsetMember(ptRefsets_.get(sab).getConcept(terminologyCodeRefsetPropertyName_.get(sab)) , cuiConcept.getPrimordialUuid(), null, Status.ACTIVE, null);
		}
		
		//sanity check on descriptions - make sure we only have one that is of type synonym with the preferred flag
		ArrayList<String> items = new ArrayList<String>();
		for (ValuePropertyPair vpp : cuiDescriptions)
		{
			//Numbers come from the rankings down below in makeDescriptionType(...)
			if (vpp.getProperty().getPropertySubType() >= BPT_Descriptions.SYNONYM && vpp.getProperty().getPropertySubType() < (BPT_Descriptions.SYNONYM + 15))
			{
				items.add(vpp.getProperty().getSourcePropertyNameFSN() + " " + vpp.getProperty().getPropertySubType());
			}
		}
		
		if (items.size() > 1)
		{
			ConsoleUtil.printErrorln("Need to rank multiple synonym types that are each marked preferred!");
			for (String s : items)
			{
				ConsoleUtil.printErrorln(s);
			}
			throw new RuntimeException("Broken assumption!");
		}
		
		List<TtkDescriptionChronicle> addedDescriptions = eConcepts_.addDescriptions(cuiConcept, cuiDescriptions);
		
		if (addedDescriptions.size() != cuiDescriptions.size())
		{
			throw new RuntimeException("oops");
		}
		
		HashSet<String> uniqueUMLSCUI = new HashSet<>();
		
		for (int i = 0; i < cuiDescriptions.size(); i++)
		{
			TtkDescriptionChronicle desc = addedDescriptions.get(i);
			ValuePropertyPairWithSAB descPP = cuiDescriptions.get(i);
			//Add attributes from SAT table
			conSat.clearParameters();
			conSat.setString(1, rxCui);
			conSat.setString(2, descPP.getStringAttribute(ptUMLSAttributes_.getProperty("RXAUI").getUUID()).get(0));  //There be 1 and only 1 of these
			ResultSet rs = conSat.executeQuery();
			
			List<BiFunction<String, String, Boolean>> functions = Arrays.asList(new BiFunction<String, String, Boolean>()
			{
				@Override
				public Boolean apply(String atn, String atv)
				{
					//Pull these up to the concept.
					if ("UMLSCUI".equals(atn))
					{
						uniqueUMLSCUI.add(atv);
						return true;
					}
					return false;
				}
			});
	
			processSAT(desc, rs, null, descPP.getSab(), functions);
		}
		
		//uniqueUMLSCUI is populated during processSAT
		for (String umlsCui : uniqueUMLSCUI)
		{
			UUID itemUUID = ConverterUUID.createNamespaceUUIDFromString("UMLSCUI" + umlsCui);
			eConcepts_.addAnnotation(cuiConcept.getConceptAttributes(), itemUUID, new TtkRefexDynamicString(umlsCui), 
					ptTermAttributes_.get("RXNORM").getProperty("UMLSCUI").getUUID(), Status.ACTIVE, null);
		}
		
		ValuePropertyPairWithAttributes.processAttributes(eConcepts_, cuiDescriptions, addedDescriptions);
		
		//there are no attributes in rxnorm without an AUI.
		
		//add semantic types
		semanticTypeStatement.clearParameters();
		semanticTypeStatement.setString(1, rxCui);
		ResultSet rs = semanticTypeStatement.executeQuery();
		processSemanticTypes(cuiConcept, rs);
		
		cuiRelStatementForward.clearParameters();
		cuiRelStatementForward.setString(1, rxCui);
		addRelationships(cuiConcept, REL.read(null, cuiRelStatementForward.executeQuery(), true, allowedCUIs, skippedRelForNotMatchingCUIFilter, this));
		
		cuiRelStatementBackward.clearParameters();
		cuiRelStatementBackward.setString(1, rxCui);
		addRelationships(cuiConcept, REL.read(null, cuiRelStatementBackward.executeQuery(), false, allowedCUIs, skippedRelForNotMatchingCUIFilter, this));

		eConcepts_.addDynamicRefsetMember(allCUIRefsetConcept_, cuiConcept.getPrimordialUuid(), null, Status.ACTIVE, null);
		cuiConcept.writeExternal(dos_);
	}

	@Override
	protected void processRelCVFAttributes(TtkRelationshipChronicle r, List<REL> duplicateRelationships)
	{
		TtkRefexDynamicMemberChronicle member = null;
		for (REL dupeRel : duplicateRelationships)
		{
			if (dupeRel.getCvf() != null)
			{
				if (dupeRel.getCvf().equals("4096"))
				{
					if (member == null)
					{
						member = eConcepts_.addDynamicRefsetMember(cpcRefsetConcept_, r.getPrimordialComponentUuid(), null, Status.ACTIVE, null);
					}
					if (dupeRel.getSourceTargetAnnotationLabel() != null)
					{
						eConcepts_.addStringAnnotation(member, dupeRel.getSourceTargetAnnotationLabel(), 
								ptRelationshipMetadata_.getProperty("sAUI & tAUI").getUUID(), Status.ACTIVE);
					}
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + dupeRel.getCvf() + "'");
				}
			}
		}
	}
	
	@Override
	protected Property makeDescriptionType(String fsnName, String preferredName, String altName, String description, final Set<String> tty_classes)
	{
		// The current possible classes are:
		// preferred
		// obsolete
		// entry_term
		// hierarchical
		// synonym
		// attribute
		// abbreviation
		// expanded
		// other

		int descriptionTypeRanking;

		//Note - ValuePropertyPairWithSAB overrides the sorting based on these values to kick RXNORM sabs to the top, where 
		//they will get used as FSN.
		if (fsnName.equals("FN") && tty_classes.contains("preferred"))
		{
			descriptionTypeRanking = BPT_Descriptions.FSN;
		}
		else if (fsnName.equals("FN"))
		{
			descriptionTypeRanking = BPT_Descriptions.FSN + 1;
		}
		// preferred gets applied with others as well, in some cases. Don't want 'preferred' 'obsolete' at the top.
		//Just preferred, and we make it the top synonym.
		else if (tty_classes.contains("preferred") && tty_classes.size() == 1)
		{
			//these sub-rankings are somewhat arbitrary at the moment, and in general, unused.  There is an error check up above which 
			//will fail the conversion if it tries to rely on these sub-rankings to find a preferred term
			int preferredSubRank;
			if (altName.equals("IN"))
			{
				preferredSubRank = 1;
			}
			else if (altName.equals("MIN"))
			{
				preferredSubRank = 2;
			}
			else if (altName.equals("PIN"))
			{
				preferredSubRank = 3;
			}
			else if (altName.equals("SCD"))
			{
				preferredSubRank = 4;
			}
			else if (altName.equals("BN"))
			{
				preferredSubRank = 5;
			}
			else if (altName.equals("SBD"))
			{
				preferredSubRank = 6;
			}
			else if (altName.equals("DF"))
			{
				preferredSubRank = 7;
			}
			else if (altName.equals("BPCK"))
			{
				preferredSubRank = 8;
			}
			else if (altName.equals("GPCK"))
			{
				preferredSubRank = 9;
			}
			else
			{
				preferredSubRank = 12;
				ConsoleUtil.printErrorln("Unranked preferred TTY type! " + fsnName + " " + altName);
			}
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + preferredSubRank;
		}
		else if (tty_classes.contains("entry_term"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 21;
		}
		else if (tty_classes.contains("synonym"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 22;
		}
		else if (tty_classes.contains("expanded"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 23;
		}
		else if (tty_classes.contains("Prescribable Name"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 24;
		}
		else if (tty_classes.contains("abbreviation"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 25;
		}
		else if (tty_classes.contains("attribute"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 26;
		}
		else if (tty_classes.contains("hierarchical"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 27;
		}
		else if (tty_classes.contains("other"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 28;
		}
		else if (tty_classes.contains("obsolete"))
		{
			descriptionTypeRanking = BPT_Descriptions.SYNONYM + 29;
		}
		else
		{
			throw new RuntimeException("Unexpected class type " + Arrays.toString(tty_classes.toArray()));
		}
		return new Property(null, fsnName, preferredName, altName, description, false, descriptionTypeRanking, null);
	}

	@Override
	protected void allDescriptionsCreated(String sab)
	{
		//noop
	}

	@Override
	protected void loadCustomMetaData() throws Exception
	{
		//noop
	}

	@Override
	protected void addCustomRefsets(BPT_MemberRefsets refset) throws Exception
	{
		refset.addProperty(cpcRefsetConceptKey_);
	}

	@Override
	protected void processSAT(TtkComponentChronicle<?> itemToAnnotate, ResultSet rs, String itemCode, String itemSab, 
			List<BiFunction<String, String, Boolean>> customHandle) throws SQLException, PropertyVetoException
	{
		
		
		while (rs.next())
		{
			//String rxcui = rs.getString("RXCUI");
			String rxaui = rs.getString("RXAUI");
			//String stype = rs.getString("STYPE");
			String code = rs.getString("CODE");
			String atui = rs.getString("ATUI");
			String satui = rs.getString("SATUI");
			String atn = rs.getString("ATN");
			String sab = rs.getString("SAB");
			String atv = rs.getString("ATV");
			String suppress = rs.getString("SUPPRESS");
			String cvf = rs.getString("CVF");
			
			
			if (customHandle != null)
			{
				boolean skipProcessing = false;
				for (BiFunction<String, String, Boolean> x : customHandle)
				{
					if (x.apply(atn, atv))
					{
						skipProcessing = true;
					}
				}
				if (skipProcessing)
				{
					continue;
				}
			}
			
			//for some reason, ATUI isn't always provided - don't know why.  must gen differently in those cases...
			UUID stringAttrUUID;
			UUID refsetUUID = ptTermAttributes_.get("RXNORM").getProperty(atn).getUUID();
			if (atui != null)
			{
				stringAttrUUID = ConverterUUID.createNamespaceUUIDFromString("ATUI" + atui);
			}
			else
			{
				//need to put the aui in here, to keep it unique, as each AUI frequently specs the same CUI
				stringAttrUUID = ConverterUUID.createNamespaceUUIDFromStrings(itemToAnnotate.getPrimordialComponentUuid().toString(), 
						rxaui, atv, refsetUUID.toString());
			}
			
			//You would expect that ptTermAttributes_.get() would be looking up sab, rather than having RxNorm hardcoded... but this is an oddity of 
			//a hack we are doing within the RxNorm load.
			TtkRefexDynamicMemberChronicle attribute = eConcepts_.addAnnotation(itemToAnnotate, stringAttrUUID, new TtkRefexDynamicString(atv), refsetUUID, Status.ACTIVE, null);
			
			if (atui != null)
			{
				eConcepts_.addStringAnnotation(attribute, atui, ptUMLSAttributes_.getProperty("ATUI").getUUID(), null);
			}
			
			//dropping for space savings
//			if (stype != null)
//			{
//				eConcepts_.addUuidAnnotation(attribute, ptSTypes_.getProperty(stype).getUUID(), ptUMLSAttributes_.getProperty("STYPE").getUUID());
//			}
			
			if (code != null && itemCode != null && !code.equals(itemCode))
			{
				throw new RuntimeException("oops");
//				if ()
//				{
//					eConcepts_.addStringAnnotation(attribute, code, ptUMLSAttributes_.getProperty("CODE").getUUID(), Status.ACTIVE);
//				}
			}

			if (satui != null)
			{
				eConcepts_.addStringAnnotation(attribute, satui, ptUMLSAttributes_.getProperty("SATUI").getUUID(), Status.ACTIVE);
			}
			
			//only load the sab if it is different than the sab of the item we are putting this attribute on
			if (sab != null && !sab.equals(itemSab))
			{
				throw new RuntimeException("Oops");
				//eConcepts_.addUuidAnnotation(attribute, ptSABs_.getProperty(sab).getUUID(), ptUMLSAttributes_.getProperty("SAB").getUUID());
			}
			if (suppress != null)
			{
				eConcepts_.addUuidAnnotation(attribute, ptSuppress_.getProperty(suppress).getUUID(), ptUMLSAttributes_.getProperty("SUPPRESS").getUUID());
			}
			if (cvf != null)
			{
				if (cvf.equals("4096"))
				{
					eConcepts_.addDynamicRefsetMember(cpcRefsetConcept_, attribute.getPrimordialComponentUuid(), null, Status.ACTIVE, null);
				}
				else
				{
					throw new RuntimeException("Unexpected value in RXNSAT cvf column '" + cvf + "'");
				}
			}
		}

		rs.close();
	}
	
	public static void main(String[] args) throws MojoExecutionException
	{
		RxNormMojo mojo = new RxNormMojo();
		mojo.outputDirectory = new File("../RxNorm-econcept/target");
		mojo.inputFileLocation = new File("../RxNorm-econcept/target/generated-resources/src/");
		mojo.execute();
	}
}
