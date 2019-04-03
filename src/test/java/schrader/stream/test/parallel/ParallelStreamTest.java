package schrader.stream.test.parallel;

import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ParallelStreamTest {

    private final String HOME = System.getenv("HOMEPATH");

    private Integer sumInt(Map.Entry<Integer, List<Integer>> entry) {
        return entry.getValue().stream().mapToInt(Integer::intValue).sum();
    }

    private Long sumLong(Map.Entry<Long, List<Long>> entry) {
        return entry.getValue().stream().mapToLong(Long::longValue).sum();
    }

    @Test
    public void testIntegerParallelStream() {
        Integer[] integerArray = {1, 1, 2, 2, 2, 3, 4, 4, 4, 4, 4, 5, 6, 7, 7, 8, 9, 7, 7, 4, 5};
        Stream.of(integerArray)
                .parallel()
                .collect(Collectors.groupingByConcurrent(Integer::intValue)) // => ConcurrentMap<Integer, List<Integer>>
                .entrySet()
                .stream()
                // .peek(System.out::println)
                .map(this::sumInt)
                // .collect(Collectors.toList())
                .forEachOrdered(System.out::println);
    }

    @Test
    public void testLongParallelStream() {
        Stream.iterate(0L, l -> l + 1).limit(1_000_000)
                .parallel()
                .collect(Collectors.groupingByConcurrent(Long::longValue))
                .entrySet()
                .stream()
                .map(this::sumLong)
                //    .collect(Collectors.toList())
                .forEachOrdered(System.out::println);
    }

    @Test
    public void testFileStream() throws URISyntaxException, IOException {
        Path in = Paths.get(getClass().getClassLoader().getResource("streams/test-100_000.log").toURI());
        try (Stream<String> lines = Files.lines(in)) {
            lines
                    .parallel()
                    .map(line -> Entry.of(line, Entry.Format.LOG_ENTRY))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(Entry::isError)
                    .collect(Collectors.groupingByConcurrent(Entry::getTimeStamp)) // groups log entries by timeStamp as Map<Long, List<LogEntry>>
                    .entrySet()
                    .stream()
                    .map(e -> new AbstractMap.SimpleEntry<>(e.getKey().getEpochSecond(), e.getValue().size())) // <= Map.Entry<Long, Integer>
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList())
                    .forEach(e -> System.out.println(e.getKey() + " " + e.getValue())); // Map.Entry::getKey, Map.Entry::getValue
        }
    }

    @Test
    public void testFileStreamModified() throws URISyntaxException, IOException {
        Path in = Paths.get(getClass().getClassLoader().getResource("streams/test-1_000_000.log").toURI());
        try (Stream<String> lines = Files.lines(in)) {
            lines
                    .parallel()
                    .map(line -> Entry.of(line, Entry.Format.LOG_ENTRY))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(Entry::isError)
                    .collect(Collectors.groupingByConcurrent(Entry::getTimeStamp, Collectors.counting())) // =>  Map<Long, Integer>
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList())
                    .forEach(e -> System.out.println(e.getKey() + " " + e.getValue())); // Map.Entry::getKey, Map.Entry::getValue
        }
    }

    @Test
    public void testWriteStreamToFile() throws IOException {
        Path out = Paths.get(HOME + "/Desktop/numbers.txt");
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(out))) {
            IntStream.range(0, 99).mapToObj(String::valueOf).forEach(pw::println);
        }
    }

    @Test
    public void testConsolidate() throws URISyntaxException, IOException {
        long start = System.currentTimeMillis();
        //
        Path in = Paths.get(getClass().getClassLoader().getResource("streams/test-1_000_000.log").toURI());
        Path out = Paths.get(HOME + "/Desktop/numbers_sort.txt");
        try (Stream<String> lines = Files.lines(in); PrintWriter pw = new PrintWriter(Files.newBufferedWriter(out))) {
            lines
                    .parallel()
                    .map(line -> Entry.of(line, Entry.Format.LOG_ENTRY))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(Entry::isError)
                    // group entries by timeStamp (result. Map<Long, List<LogEnry>) and count values (result: Map<Long, Integer>)
                    .collect(Collectors.groupingByConcurrent(Entry::getTimeStamp, Collectors.counting()))
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList())
                    .forEach(e -> pw.println(e.getKey() + " " + e.getValue()));
        }
        //
        long stop = System.currentTimeMillis();
        System.out.println(stop - start + " ms");
    }

    @Test
    public void testConsolidateWithoutOrdering() throws URISyntaxException, IOException {
        long start = System.currentTimeMillis();
        //
        Path in = Paths.get(getClass().getClassLoader().getResource("streams/test-1_000_000.log").toURI());
        Path out = Paths.get(HOME + "/Desktop/numbers_par.txt");
        try (Stream<String> lines = Files.lines(in); PrintWriter pw = new PrintWriter(Files.newBufferedWriter(out))) {
            lines
                    .parallel()
                    .map(line -> Entry.of(line, Entry.Format.LOG_ENTRY))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(Entry::isError)
                    // Group entries by timeStamp:
                    // (1) 'groupingBy' generates a Map<Long, List<LogEnry>
                    // (2) 'Collectors.counting() generates a Map<Long, Long> by counting the entries in 'List<LogEnry>'
                    // (3) 'LinkedHashMap::new' keeps the order to avoid subsequent sorting
                    .collect(Collectors.groupingBy(Entry::getTimeStamp, LinkedHashMap::new, Collectors.counting()))
                    .forEach((k, v) -> pw.println(k + " " + v));
        }
        //
        long stop = System.currentTimeMillis();
        System.out.println(stop - start + " ms");
    }
}
