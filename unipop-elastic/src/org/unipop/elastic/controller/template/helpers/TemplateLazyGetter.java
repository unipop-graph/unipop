package org.unipop.elastic.controller.template.helpers;

import org.elasticsearch.client.Client;
import org.elasticsearch.script.ScriptService;
import org.unipop.elastic.helpers.ElasticLazyGetter;
import org.unipop.elastic.helpers.TimingAccessor;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateLazyGetter extends ElasticLazyGetter {

    public TemplateLazyGetter(Client client, TimingAccessor timing) {
        super(client, timing);
    }

    @Override
    public void execute() {
        //TODO: implement lazy getter for multiple templates
    }
}
