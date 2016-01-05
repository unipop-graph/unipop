package org.unipop.elastic2.schema.misc;

import org.apache.commons.lang3.StringUtils;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementRouting;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic2.controller.schema.helpers.schemaProviders.GraphVertexSchema;

import java.util.*;

/**
 * Created by Gilad on 12/10/2015.
 */
public class ModernGraphElementSchemaProvider implements GraphElementSchemaProvider {
    //region Constructor
    public ModernGraphElementSchemaProvider(String indexName) {
        this.indexName = indexName;

        this.vertexTypes = new HashSet<>(Arrays.asList(
                "person",
                "software",
                "vertex",
                "dog",
                "song",
                "artist"
        ));

        this.edgeTypes =  new HashSet<>(Arrays.asList(
                "knows",
                "created",
                "test",
                "pets",
                "walks",
                "livesWith",
                "friend",
                "friends",
                "collaborator",
                "hate",
                "hates",
                "self",
                "link",
                "existsWith",
                "co-developer",
                "sungBy",
                "writtenBy",
                "followedBy"
        ));
    }
    //endregion

    //region GraphElementSchemaProvider implementation
    @Override
    public Optional<GraphVertexSchema> getVertexSchema(String type) {
        if (StringUtils.isBlank(type)) {
            return Optional.empty();
        }

        switch (type) {
            case "person": return getPersonSchema();
            case "software": return getSoftwareSchema();
            case "vertex": return getModernVertexSchema();
            case "dog": return getDogSchema();
            default:
                this.vertexTypes.add(type);
                return getDefaultVertexSchema(type);
        }
    }

    @Override
    public Optional<GraphEdgeSchema> getEdgeSchema(
            String type,
            Optional<String> sourceType,
            Optional<String> destinationType) {

        if (StringUtils.isBlank(type)) {
            return Optional.empty();
        }

        switch (type) {
            case "knows":
                if ((sourceType != null && sourceType.isPresent() && sourceType.get().equals("person")) ||
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("person"))) {
                    return getPersonKnowsPersonSchema();
                }

                if ((sourceType != null && sourceType.isPresent() && sourceType.get().equals("vertex")) ||
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("vertex"))) {
                    return getVertexKnowsVertexSchema();
                }

                return Optional.empty();

            case "created":
                if ((sourceType != null && sourceType.isPresent() && sourceType.get().equals("person")) ||
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("software"))) {
                    return getPersonCreatedSoftwareSchema();
                }

                if ((sourceType != null && sourceType.isPresent() && sourceType.get().equals("vertex")) ||
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("vertex"))) {
                    return getVertexCreatedVertexSchema();
                }

                return Optional.empty();

            case "test": return getVertexTestVertexSchema();
            case "pets": return getVertexPetsVertexSchema();
            case "walks": return getVertexWalksVertexSchema();
            case "livesWith": return getVertexLivesWithVertexSchema();
            case "friend": return getVertexFriendVertexSchema();
            case "friends": return getPersonFriendsPersonSchema();
            case "collaborator": return getVertexCollaboratorVertexSchema();
            case "hate": return getVertexHateVertexSchema();
            case "hates": return getVertexHatesVertexSchema();
            case "self": return getVertexSelfVertexSchema();
            case "link": return getVertexLinkVertexSchema();

            case "existsWith":
                if ((sourceType != null && sourceType.isPresent() && sourceType.get().equals("person")) &&
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("person"))) {
                    return getPersonExistsWithPersonSchema();
                }

                if ((sourceType != null && sourceType.isPresent() && sourceType.get().equals("software")) &&
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("software"))) {
                    return getSoftwareExistsWithSoftwareSchema();
                }

                if (sourceType != null && sourceType.isPresent() && sourceType.get().equals("person") &&
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("software"))){
                    return getPersonExistsWithSoftwareSchema();
                }

                if (sourceType != null && sourceType.isPresent() && sourceType.get().equals("software") &&
                        (destinationType != null && destinationType.isPresent() && destinationType.get().equals("person"))){
                    return getSoftwareExistsWithPersonSchema();
                }

                if (sourceType != null && sourceType.isPresent() && sourceType.get().equals("person")){
                    return getPersonExistsWithPersonSchema();
                }

                if (sourceType != null && sourceType.isPresent() && sourceType.get().equals("software")){
                    return getSoftwareExistsWithSoftwareSchema();
                }

                if (destinationType != null && destinationType.isPresent() && destinationType.get().equals("person")){
                    return getPersonExistsWithPersonSchema();
                }

                if (destinationType != null && destinationType.isPresent() && destinationType.get().equals("software")){
                    return getSoftwareExistsWithSoftwareSchema();
                }

                return Optional.empty();

            case "co-developer": return getPersonCoDeveloperWithPersonSchema();
            case "followedBy": return getFollowedBySchema();
            case "writtenBy": return getWrittenBySchema();
            case "sungBy": return getSungBySchema();
            default:
                this.edgeTypes.add(type);
                return getDefaultEdgeSchema(type);
        }
    }

    @Override
    public Optional<Iterable<GraphEdgeSchema>> getEdgeSchemas(String type) {
        if (StringUtils.isBlank(type)) {
            return Optional.empty();
        }

        switch (type) {
            case "knows": return Optional.of(Arrays.asList(getPersonKnowsPersonSchema().get(), getVertexKnowsVertexSchema().get()));
            case "created": return Optional.of(Arrays.asList(getPersonCreatedSoftwareSchema().get(), getVertexCreatedVertexSchema().get()));
            case "test": return Optional.of(Arrays.asList(getVertexTestVertexSchema().get()));
            case "pets": return Optional.of(Arrays.asList(getVertexPetsVertexSchema().get()));
            case "walks": return Optional.of(Arrays.asList(getVertexWalksVertexSchema().get()));
            case "livesWith": return Optional.of(Arrays.asList(getVertexLivesWithVertexSchema().get()));
            case "friend": return Optional.of(Arrays.asList(getVertexFriendVertexSchema().get()));
            case "friends": return Optional.of(Arrays.asList(getPersonFriendsPersonSchema().get()));
            case "collaborator": return Optional.of(Arrays.asList(getVertexCollaboratorVertexSchema().get()));
            case "hate": return Optional.of(Arrays.asList(getVertexHateVertexSchema().get()));
            case "hates": return Optional.of(Arrays.asList(getVertexHatesVertexSchema().get()));
            case "self": return Optional.of(Arrays.asList(getVertexSelfVertexSchema().get()));
            case "link": return Optional.of(Arrays.asList(getVertexLinkVertexSchema().get()));
            case "existsWith": return Optional.of(Arrays.asList(getPersonExistsWithPersonSchema().get(), getPersonExistsWithSoftwareSchema().get(), getSoftwareExistsWithSoftwareSchema().get(), getSoftwareExistsWithPersonSchema().get()));
            case "co-developer": return Optional.of(Arrays.asList(getPersonCoDeveloperWithPersonSchema().get()));
            case "followedBy": return Optional.of(Arrays.asList(getFollowedBySchema().get()));
            case "writtenBy": return Optional.of(Arrays.asList(getWrittenBySchema().get()));
            case "sungBy": return Optional.of(Arrays.asList(getSungBySchema().get()));
            default: return Optional.of(Arrays.asList(getDefaultEdgeSchema(type).get()));
        }
    }

    @Override
    public Iterable<String> getVertexTypes() {
        return this.vertexTypes;
    }

    @Override
    public Iterable<String> getEdgeTypes() {
        return this.edgeTypes;
    }
    //endregion

    //region Private Methods
    private Optional<GraphVertexSchema> getSoftwareSchema() {
        return Optional.of(new GraphVertexSchema() {
            @Override
            public String getType() {
                return "software";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphVertexSchema> getPersonSchema() {
        return Optional.of(new GraphVertexSchema() {
            @Override
            public String getType() {
                return "person";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphVertexSchema> getModernVertexSchema() {
        return Optional.of(new GraphVertexSchema() {
            @Override
            public String getType() {
                return "vertex";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphVertexSchema> getDogSchema() {
        return Optional.of(new GraphVertexSchema() {
            @Override
            public String getType() {
                return "dog";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphVertexSchema> getDefaultVertexSchema(String type) {
        return Optional.of(new GraphVertexSchema() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getPersonKnowsPersonSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "knows";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexKnowsVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "knows";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getPersonCreatedSoftwareSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "softwareId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("software");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "created";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexCreatedVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "created";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexTestVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "test";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexPetsVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "pets";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexWalksVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "walks";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexLivesWithVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "livesWith";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexFriendVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "friend";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getPersonFriendsPersonSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "friends";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexCollaboratorVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "collaborator";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexHateVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "hate";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexHatesVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "hates";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexSelfVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "self";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getVertexLinkVertexSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "link";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getPersonExistsWithPersonSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "existsWith";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getPersonExistsWithSoftwareSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "softwareIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("software");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "existsWith";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getSoftwareExistsWithPersonSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "softwareIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("software");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "existsWith";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getSoftwareExistsWithSoftwareSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "softwareIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("software");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "softwareIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("software");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "existsWith";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getPersonCoDeveloperWithPersonSchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "personIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "co-developer";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    public Optional<GraphEdgeSchema> getFollowedBySchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "songIdA";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("song");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "songIdB";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("song");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "followedBy";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    public Optional<GraphEdgeSchema> getWrittenBySchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "songId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("song");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "artistId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("artist");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "writtenBy";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    public Optional<GraphEdgeSchema> getSungBySchema() {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "songId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("song");
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "artistId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("artist");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "sungBy";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }

    private Optional<GraphEdgeSchema> getDefaultEdgeSchema(String type) {
        return Optional.of(new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdA";
                    }

                    @Override
                    public Optional<String> getType() { return Optional.of("vertex"); }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "vertexIdB";
                    }

                    @Override
                    public Optional<String> getType() { return Optional.of("vertex");
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return type;
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(
                        indexName
                );
            }
        });
    }
    //endregion

    //region Fields
    protected Set<String> vertexTypes;
    protected Set<String> edgeTypes;

    protected String indexName;
    //endregion
}

