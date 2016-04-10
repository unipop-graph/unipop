package org.unipop.query.controller;

import org.apache.commons.configuration.Configuration;
import org.json.JSONObject;
import org.unipop.structure.UniGraph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

public class ConfigurationControllerManager implements ControllerManager {

    protected Set<SourceProvider> sourceProviders = new HashSet<>();
    protected Set<UniQueryController> controllers = new HashSet<>();

    public ConfigurationControllerManager(UniGraph graph, Configuration configuration) throws Exception {
        String[] providers = configuration.getStringArray("providers");
        if(providers.length == 0) throw new InstantiationException("No 'mappings' configured for ConfigurationControllerManager");
        for(String provider : providers){
            String providerJson = readFile(provider);
            JSONObject providerConfig = new JSONObject(providerJson);
            String providerClass = providerConfig.getString("class");
            SourceProvider sourceProvider = Class.forName(providerClass).asSubclass(SourceProvider.class).newInstance();
            Set<UniQueryController> controllers = sourceProvider.init(graph, providerConfig);
            this.controllers.addAll(controllers);
            this.sourceProviders.add(sourceProvider);
        }
    }

    private static String readFile(String filename) {
        String result = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Set<UniQueryController> getControllers() {
        return controllers;
    }

    @Override
    public void close() {
        sourceProviders.forEach(SourceProvider::close);
    }
}
