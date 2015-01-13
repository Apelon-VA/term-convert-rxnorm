package gov.va.rxnorm.propertyTypes;

import gov.va.oia.terminology.converters.sharedUtils.propertyTypes.BPT_IDs;

/**
 * Properties from the DTS ndf load which are treated as alternate IDs within the workbench.
 * @author Daniel Armbrust
 */
public class PT_IDs extends BPT_IDs
{
	public PT_IDs()
	{
		super();
		indexByAltNames();
		addProperty("RxNorm Unique identifier for concept", null, "RXCUI", "(RxNorm Concept ID)");
		addProperty("Unique identifier for atom", null, "RXAUI", "(RxNorm Atom ID)");  //loaded as an attribute and a id
		addProperty("Unique identifier of Semantic Type", null, "TUI", null);
		addProperty("Unique identifier for Relationship", null, "RUI", null);
		addProperty("Unique identifier for attribute", null, "ATUI", null);
	}
}
