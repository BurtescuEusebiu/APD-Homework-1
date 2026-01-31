import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadSalahor implements Runnable {
    private final ArrayList<String> articlesPath;
    private final List<List<Article>> sortedLocalArticles;
    private final ConcurrentHashMap<String, Boolean> articlesUUID;
    private final ConcurrentHashMap<String, Boolean> articlesTitle;
    private final ConcurrentHashMap<String, Article> dupes;
    private final CyclicBarrier barrierRead;
    private final CyclicBarrier barrierWrite;
    private final String path;
    private final ArrayList<String> languages;
    private final ArrayList<String> categories;
    private final ArrayList<String> linkingWords;
    private final int threadID;
    private final ConcurrentHashMap<String, ConcurrentSkipListSet<String>> categorisedArticles;
    private final ConcurrentHashMap<String, ConcurrentSkipListSet<String>> languagesArticles;
    private final List<List<Map.Entry<String,Integer>>> sortedLocalWordsCounter;
    private final AtomicInteger nrTotalArticles;
    private final ConcurrentHashMap<String,Integer> authors;

    public ThreadSalahor(
            int threadID,
            String path,
            ArrayList<String> articlesPath,
            ArrayList<String> languages,
            ArrayList<String> categories,
            ArrayList<String> linkingWords,
            ConcurrentHashMap<String, ConcurrentSkipListSet<String>> categorisedArticles,
            ConcurrentHashMap<String, ConcurrentSkipListSet<String>> languagesArticles,
            List<List<Map.Entry<String,Integer>>> sortedLocalWordsCounter,
            List<List<Article>> sortedLocalArticles,
            ConcurrentHashMap<String, Boolean> articlesUUID,
            ConcurrentHashMap<String, Boolean> articlesTitle,
            ConcurrentHashMap<String, Article> dupes,
            CyclicBarrier barrierRead,
            CyclicBarrier barrierWrite,
            AtomicInteger nrTotalArticles,
            ConcurrentHashMap<String,Integer> authors
    ) {
        this.path = path;
        this.threadID = threadID;
        this.articlesPath = articlesPath;
        this.sortedLocalArticles = sortedLocalArticles;
        this.articlesUUID = articlesUUID;
        this.articlesTitle = articlesTitle;
        this.dupes = dupes;
        this.categorisedArticles = categorisedArticles;
        this.languagesArticles = languagesArticles;
        this.sortedLocalWordsCounter = sortedLocalWordsCounter;
        this.barrierRead = barrierRead;
        this.barrierWrite = barrierWrite;
        this.languages = languages;
        this.categories = categories;
        this.linkingWords = linkingWords;
        this.nrTotalArticles = nrTotalArticles;
        this.authors = authors;
    }

    @Override
    public void run() {
        List<Article> localArticles = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            for (String articleFile : articlesPath) {
                Path fullPath = Paths.get(path).getParent().resolve(articleFile).normalize();
                List<Article> fileArticles = mapper.readValue(
                        new FileReader(fullPath.toFile()),
                        new TypeReference<List<Article>>() {}
                );

                for (Article a : fileArticles) {
                    String uuid = a.uuid;
                    String title = a.title;

                    boolean isNewUUID = articlesUUID.putIfAbsent(uuid, Boolean.TRUE) == null;
                    boolean isNewTitle = articlesTitle.putIfAbsent(title, Boolean.TRUE) == null;

                    nrTotalArticles.incrementAndGet();

                    if (isNewUUID && isNewTitle) {
                        localArticles.add(a);
                    } else {
                        dupes.put(uuid + "::" + title, a);
                    }
                }
            }

            barrierRead.await();

            Set<String> dupUUIDs = new HashSet<>();
            Set<String> dupTitles = new HashSet<>();
            for (Article d : dupes.values()) {
                dupUUIDs.add(d.uuid);
                dupTitles.add(d.title);
            }

            localArticles.removeIf(a -> dupUUIDs.contains(a.uuid) || dupTitles.contains(a.title));

            localArticles.sort((a, b) -> {
                int cmp = b.published.compareTo(a.published);
                return (cmp != 0) ? cmp : a.uuid.compareTo(b.uuid);
            });

            sortedLocalArticles.set(threadID, localArticles);

            Map<String,Integer> localWordsCounter = new HashMap<>();

            for (Article a : localArticles) {
                String uuid = a.uuid;

                for(String c : a.categories) {
                    categorisedArticles.putIfAbsent(c, new ConcurrentSkipListSet<>());
                    categorisedArticles.get(c).add(uuid);
                }

                String language = a.language;
                languagesArticles.putIfAbsent(language, new ConcurrentSkipListSet<>());
                languagesArticles.get(language).add(uuid);

                if ("english".equalsIgnoreCase(a.language)) {
                    HashSet<String> wordsInArticle = new HashSet<>();
                    String text = a.text == null ? "" : a.text.toLowerCase();
                    String[] words = text.split("\\s+");

                    for (String word : words) {
                        String cleaned = word.replaceAll("[^a-z]", "");
                        if (cleaned.length() == 0) continue;
                        if (linkingWords.contains(cleaned)) continue;
                        wordsInArticle.add(cleaned);
                    }

                    for (String w : wordsInArticle) {
                        localWordsCounter.merge(w, 1, Integer::sum);
                    }
                }

                authors.merge(a.author, 1, Integer::sum);
            }

            List<Map.Entry<String,Integer>> localSortedWords =
                    new ArrayList<>(localWordsCounter.entrySet());

            localSortedWords.sort((a, b) -> {
                int cmp = Integer.compare(b.getValue(), a.getValue());
                return (cmp != 0) ? cmp : a.getKey().compareTo(b.getKey());
            });

            sortedLocalWordsCounter.set(threadID, localSortedWords);

            barrierWrite.await();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
