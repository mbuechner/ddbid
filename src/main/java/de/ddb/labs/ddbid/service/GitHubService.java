/*
 * Copyright 2022 Michael Büchner, Deutsche Digitale Bibliothek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ddb.labs.ddbid.service;

import static com.github.davidmoten.bigsorter.Serializer.java;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.DepthWalk.RevWalk;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class GitHubService {

    public final static String FILE_NAME = "ddb-dataset-provider.csv";
    private final DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE;

    @Value(value = "${ddbid.git.url}")
    private String GIT_URL;

    @Value(value = "${ddbid.git.branch}")
    private String GIT_BRANCH;

    @Value(value = "${ddbid.git.name}")
    private String GIT_NAME;

    @Value(value = "${ddbid.git.pass}")
    private String GIT_PASS;

    private Git git;
    private Repository repository;
    @Getter
    private boolean init = false;

    public void init() throws GitAPIException, IOException {
        // make local folder
        final Path folder = Files.createTempDirectory("git");
        folder.toFile().deleteOnExit();

        log.info("Clone Branch {} of {} to {}", GIT_BRANCH, GIT_URL, folder.toString());
        this.git = Git.cloneRepository()
                .setURI(GIT_URL)
                .setDirectory(folder.toFile())
                .setBranchesToClone(Arrays.asList(GIT_BRANCH))
                .setBranch(GIT_BRANCH)
                .call();
        this.repository = git.getRepository();
        this.init = true;
    }

    public Map<String, String> getCommits() throws IOException, GitAPIException {

        if (!isInit()) {
            init();
        }

        final Map<String, String> map = new HashMap<>();
        try ( RevWalk revWalk = new RevWalk(repository, -1)) {
            final Iterable<RevCommit> commits = git.log().all().call();
            for (RevCommit commit : commits) {
                final RevCommit targetCommit = revWalk.parseCommit(repository.resolve(commit.getName()));
                for (Ref e : repository.getRefDatabase().getRefs()) {
                    if (e.getName().startsWith(Constants.R_HEADS) && revWalk.isMergedInto(targetCommit, revWalk.parseCommit(e.getObjectId()))) {
                        final String foundInBranch = e.getName();
                        if (GIT_BRANCH.equals(foundInBranch)) {
                            final String date = dtf.format(new Timestamp(commit.getCommitTime() * 1000L).toLocalDateTime());
                            map.put(commit.getName(), date);
                            break;
                        }
                    }
                }
            }
        }
        final LinkedHashMap<String, String> reverseSortedMap = new LinkedHashMap<>();
        map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        return reverseSortedMap;
    }

    public InputStream getFile(String commitId) throws IncorrectObjectTypeException, CorruptObjectException, IOException, GitAPIException {
        if (!isInit()) {
            init();
        }
        final ObjectId lastCommitId = repository.resolve(commitId);
        try ( RevWalk revWalk = new RevWalk(repository, -1)) {
            final RevCommit commit = revWalk.parseCommit(lastCommitId);
            // and using commit's tree find the path
            final RevTree tree = commit.getTree();

            // now try to find a specific file
            try ( TreeWalk treeWalk = new TreeWalk(repository)) {
                treeWalk.addTree(tree);
                treeWalk.setRecursive(true);
                treeWalk.setFilter(PathFilter.create(FILE_NAME));
                if (!treeWalk.next()) {
                    throw new IllegalStateException("Did not find expected file 'README.md'");
                }

                final ObjectId objectId = treeWalk.getObjectId(0);
                final ObjectLoader loader = repository.open(objectId);
                return loader.openStream();
            } finally {
                revWalk.dispose();
            }
        }
    }

    public void commit(Path file) throws IOException, GitAPIException {

        if (!isInit()) {
            init();
        }

        log.info("Kopiere " + file.getFileName() + " zu " + FILE_NAME + "...");
        Files.copy(file, Paths.get(repository.getDirectory().getParent(), FILE_NAME), StandardCopyOption.REPLACE_EXISTING);

        log.info("Git-Befehl ADD...");
        git.add()
                .addFilepattern(FILE_NAME)
                .call();
        log.info("Git-Befehl COMMIT...");
        git.commit()
                .setAll(true)
                .setMessage("Aktualisierung von " + FILE_NAME + " am " + dtf.format(Instant.now()) + ".")
                .call();
        log.info("Git-Befehl PUSH...");
        final PushCommand push = git.push();
        push.setCredentialsProvider(new UsernamePasswordCredentialsProvider(GIT_NAME, GIT_PASS));
        push.call();
        log.info("Erfolgreich auf GitHub veröffentlicht!");
    }

    public void close() {
        if (isInit()) {
            repository.close();
            git.close();
        }
    }
}
