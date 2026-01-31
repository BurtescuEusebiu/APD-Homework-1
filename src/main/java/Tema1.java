import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.min;

public class Tema1 {
    public static int nrThreads;
    public static String fisierArticol;
    public static String fisierSuplimentar;
    public static ArrayList<String> articlesPaths;
    public static ArrayList<String> categories;
    public static ConcurrentHashMap<String, ConcurrentSkipListSet<String>> categorisedArticles;
    public static ConcurrentHashMap<String, ConcurrentSkipListSet<String>> languagesArticles;
    public static ConcurrentHashMap<String, Integer> authors;
    public static List<Map.Entry<String, Integer>> wordsCounter;
    public static ArrayList<String> languages;
    public static ArrayList<String> linkingWords;
    public static int nrArticles;
    public static ArrayList<Thread> threads;
    public static AtomicInteger nrTotalArticles;
    public static List<List<Article>> sortedLocalArticles;
    public static List<List<Map.Entry<String, Integer>>> sortedLocalWordsCounter;
    public static List<Article> articlesTotal;

    public static List<Article> mergeSortedLists(List<List<Article>> sortedLists) {
        List<Article> merged = new ArrayList<>();
        PriorityQueue<int[]> pq = new PriorityQueue<>((a, b) -> {
            Article a1 = sortedLists.get(a[0]).get(a[1]);
            Article b1 = sortedLists.get(b[0]).get(b[1]);
            int cmp = b1.published.compareTo(a1.published);
            if (cmp != 0) return cmp;
            return a1.uuid.compareTo(b1.uuid);
        });

        for (int i = 0; i < sortedLists.size(); i++) {
            if (sortedLists.get(i) != null && !sortedLists.get(i).isEmpty()) {
                pq.add(new int[]{i, 0});
            }
        }

        while (!pq.isEmpty()) {
            int[] top = pq.poll();
            Article a = sortedLists.get(top[0]).get(top[1]);
            merged.add(a);

            int next = top[1] + 1;
            if (next < sortedLists.get(top[0]).size()) {
                pq.add(new int[]{top[0], next});
            }
        }

        return merged;
    }

    public static List<Map.Entry<String, Integer>> mergeSortedWordLists(List<List<Map.Entry<String, Integer>>> sortedLists) {
        Map<String, Integer> globalCount = new HashMap<>();
        for (List<Map.Entry<String, Integer>> localList : sortedLists) {
            if (localList == null) continue;
            for (Map.Entry<String, Integer> entry : localList) {
                globalCount.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }
        List<Map.Entry<String, Integer>> merged = new ArrayList<>(globalCount.entrySet());
        merged.sort((a, b) -> {
            int cmp = Integer.compare(b.getValue(), a.getValue());
            if (cmp != 0) return cmp;
            return a.getKey().compareTo(b.getKey());
        });
        return merged;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        nrThreads = Integer.parseInt(args[0]);
        fisierArticol = args[1];
        fisierSuplimentar = args[2];

//        nrThreads = 16;
//        fisierArticol = "../checker/input/tests/test_5/articles.txt";
//        fisierSuplimentar = "../checker/input/tests/test_5/inputs.txt";

        articlesTotal = new ArrayList<>();
        ConcurrentHashMap<String, Boolean> articlesUUID = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Boolean> articlesTitle = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Article> dupes = new ConcurrentHashMap<>();
        categorisedArticles = new ConcurrentHashMap<>();
        languagesArticles = new ConcurrentHashMap<>();
        wordsCounter = new ArrayList<>();
        nrTotalArticles = new AtomicInteger(0);
        authors = new ConcurrentHashMap<>();

        sortedLocalArticles = new ArrayList<>(nrThreads);
        for (int i = 0; i < nrThreads; i++) sortedLocalArticles.add(null);

        sortedLocalWordsCounter = new ArrayList<>(nrThreads);
        for (int i = 0; i < nrThreads; i++) sortedLocalWordsCounter.add(null);

        CyclicBarrier barrierRead = new CyclicBarrier(nrThreads);
        CyclicBarrier barrierWrite = new CyclicBarrier(nrThreads, () -> {
            try {
                articlesTotal = mergeSortedLists(sortedLocalArticles);
                wordsCounter = mergeSortedWordLists(sortedLocalWordsCounter);

                try (BufferedWriter writer = new BufferedWriter(new FileWriter("all_articles.txt"))) {
                    for (Article a : articlesTotal) {
                        writer.write(a.uuid + " " + a.published);
                        writer.newLine();
                    }
                }

                for (String c : categories) {
                    if (categorisedArticles.containsKey(c) && !categorisedArticles.get(c).isEmpty()) {
                        String filename = c.replace(",", "").replace(" ", "_") + ".txt";
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                            for (String a : categorisedArticles.get(c)) {
                                writer.write(a);
                                writer.newLine();
                            }
                        }
                    }
                }

                for (String l : languages) {
                    if (languagesArticles.containsKey(l) && !languagesArticles.get(l).isEmpty()) {
                        String filename = l + ".txt";
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                            for (String a : languagesArticles.get(l)) {
                                writer.write(a);
                                writer.newLine();
                            }
                        }
                    }
                }

                try (BufferedWriter writer = new BufferedWriter(new FileWriter("keywords_count.txt"))) {
                    for (Map.Entry<String, Integer> entry : wordsCounter) {
                        writer.write(entry.getKey() + " " + entry.getValue());
                        writer.newLine();
                    }
                }

                try (BufferedWriter writer = new BufferedWriter(new FileWriter("reports.txt"))) {
                    writer.write("duplicates_found - " + (nrTotalArticles.get() - articlesTotal.size()) + '\n');
                    writer.write("unique_articles - " + articlesTotal.size() + '\n');

                    String bestAuthor = null;
                    int maxArticles = 0;
                    for (Map.Entry<String, Integer> entry : authors.entrySet()) {
                        if (entry.getValue() > maxArticles) {
                            maxArticles = entry.getValue();
                            bestAuthor = entry.getKey();
                        } else if (entry.getValue() == maxArticles && bestAuthor != null) {
                            if (entry.getKey().compareTo(bestAuthor) < 0) {
                                bestAuthor = entry.getKey();
                            }
                        }
                    }
                    writer.write("best_author - " + bestAuthor + " " + maxArticles + '\n');

                    String bestLanguage = null;
                    maxArticles = 0;
                    for (Map.Entry<String, ConcurrentSkipListSet<String>> entry : languagesArticles.entrySet()) {
                        int size = entry.getValue().size();
                        if (size > maxArticles) {
                            maxArticles = size;
                            bestLanguage = entry.getKey();
                        } else if (size == maxArticles && bestLanguage != null) {
                            if (entry.getKey().compareTo(bestLanguage) < 0) bestLanguage = entry.getKey();
                        }
                    }
                    writer.write("top_language - " + bestLanguage + " " + maxArticles + '\n');

                    String bestCategory = null;
                    maxArticles = 0;
                    for (Map.Entry<String, ConcurrentSkipListSet<String>> entry : categorisedArticles.entrySet()) {
                        int size = entry.getValue().size();
                        if (size > maxArticles) {
                            maxArticles = size;
                            bestCategory = entry.getKey();
                        } else if (size == maxArticles && bestCategory != null) {
                            bestCategory = entry.getKey();
                        }
                    }
                    writer.write("top_category - " + (bestCategory == null ? "" : bestCategory.replace(",", "").replace(" ", "_")) + " " + maxArticles + '\n');

                    Article mostRecent = articlesTotal.isEmpty() ? null : articlesTotal.get(0);
                    String mostRecentArticle = mostRecent == null ? "" : mostRecent.url;
                    String mostRecentTime = mostRecent == null ? "" : mostRecent.published;

                    String bestWord = wordsCounter.isEmpty() ? "" : wordsCounter.get(0).getKey();
                    int nr = wordsCounter.isEmpty() ? 0 : wordsCounter.get(0).getValue();

                    writer.write("most_recent_article - " + mostRecentTime + " " + mostRecentArticle + '\n');
                    writer.write("top_keyword_en - " + bestWord + " " + nr + '\n');
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        BufferedReader br = new BufferedReader(new FileReader(fisierArticol));
        nrArticles = Integer.parseInt(br.readLine());
        articlesPaths = new ArrayList<>();
        for (int i = 0; i < nrArticles; i++) articlesPaths.add(br.readLine());
        br.close();

        br = new BufferedReader(new FileReader(fisierSuplimentar));
        br.readLine();
        String languagesString = br.readLine();
        String categoriesString = br.readLine();
        String linkingString = br.readLine();
        br.close();

        Path languagesPath = Paths.get(fisierSuplimentar).getParent().resolve(languagesString);
        br = new BufferedReader(new FileReader(languagesPath.toFile()));
        languages = new ArrayList<>();
        int nrLanguages = Integer.parseInt(br.readLine());
        for (int i = 0; i < nrLanguages; i++) languages.add(br.readLine());
        br.close();

        Path categoriesPath = Paths.get(fisierSuplimentar).getParent().resolve(categoriesString);
        br = new BufferedReader(new FileReader(categoriesPath.toFile()));
        categories = new ArrayList<>();
        int nrCategories = Integer.parseInt(br.readLine());
        for (int i = 0; i < nrCategories; i++) categories.add(br.readLine());
        br.close();

        Path linkingPath = Paths.get(fisierSuplimentar).getParent().resolve(linkingString);
        br = new BufferedReader(new FileReader(linkingPath.toFile()));
        linkingWords = new ArrayList<>();
        int nrLinkingWords = Integer.parseInt(br.readLine());
        for (int i = 0; i < nrLinkingWords; i++) linkingWords.add(br.readLine());
        br.close();

        threads = new ArrayList<>();
        for (int i = 0; i < nrThreads; i++) {
            int start = i * nrArticles / nrThreads;
            int end = min((i + 1) * nrArticles / nrThreads, nrArticles);
            ArrayList<String> currThreadArticles = new ArrayList<>(articlesPaths.subList(start, end));
            Thread t = new Thread(
                    new ThreadSalahor(
                            i,
                            fisierArticol,
                            currThreadArticles,
                            languages,
                            categories,
                            linkingWords,
                            categorisedArticles,
                            languagesArticles,
                            sortedLocalWordsCounter,
                            sortedLocalArticles,
                            articlesUUID,
                            articlesTitle,
                            dupes,
                            barrierRead,
                            barrierWrite,
                            nrTotalArticles,
                            authors
                    )
            );
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < nrThreads; i++) threads.get(i).join();
    }
}
