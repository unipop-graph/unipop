package org.unipop.util;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.TimerTask;

/**
 * Created by 2xEsh on 5/27/2018.
 */
public class GitWatcher {
    private Git git;
    private Repository repository;
    private boolean needUpdate;
    private int interval;

    public GitWatcher(String gitRemote, Path localPath, int interval) throws GitAPIException, IOException {
        File repoDirectory = new File(String.valueOf(localPath));
        this.interval = interval;

        if (!repoDirectory.exists()) {
            this.git = Git.cloneRepository()
                    .setURI(gitRemote)
                    .setDirectory(repoDirectory)
                    .call();
            this.repository = git.getRepository();
        }
        else {
            this.git = Git.open(repoDirectory);
            this.repository = git.getRepository();
        }
    }

    public void start(){
        java.util.Timer t = new java.util.Timer();
        t.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    ObjectId oldHead = repository.resolve("HEAD^{tree}");
                    git.pull().call();
                    ObjectId head = repository.resolve("HEAD^{tree}");

                    ObjectReader reader = repository.newObjectReader();
                    CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
                    oldTreeIter.reset(reader, oldHead);
                    CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
                    newTreeIter.reset(reader, head);
                    List<DiffEntry> diffs = git.diff()
                            .setNewTree(newTreeIter)
                            .setOldTree(oldTreeIter)
                            .call();

                    if (diffs.size() > 0)
                        needUpdate = true;

                } catch (IOException | GitAPIException e) {
                    e.printStackTrace();
                }
            }
        }, this.interval, this.interval);
    }
    public boolean isNeedUpdateAndReset(){
        boolean shouldUpdate = this.needUpdate;
        if (shouldUpdate){
            this.needUpdate = false;
        }
        return shouldUpdate;
    }
}
