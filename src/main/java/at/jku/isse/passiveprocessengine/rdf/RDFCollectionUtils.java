package at.jku.isse.passiveprocessengine.rdf;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Seq;

public class RDFCollectionUtils {

	/**
	 * @param list a Seq containing only resource elements, caller must ensure it contains no literals!
	 * @param model from which to generate the individuals from
	 * @return list of individuals in same order as list Seq
	 */
	public static List<OntIndividual> mapSeqOfResourcesToListOfIndividuals(Seq list, OntModel model) {
		NodeIterator iter = list.iterator();
		List<OntIndividual> elements = new ArrayList<>();
		while(iter.hasNext()) {
			elements.add(model.getIndividual(iter.next().asResource().getURI()));
		}
		return elements;
	}
	

	
}
