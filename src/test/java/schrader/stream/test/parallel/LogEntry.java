package schrader.stream.test.parallel;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class Entry {

    Instant timeStamp;
    String level;

    static Optional<Entry> of(String line, Format format) {
        if (format == Format.LOG_ENTRY) {
            return Optional.of(new LogEntry(line));
        }
        return Optional.empty();
    }

    Instant getTimeStamp() {
        return this.timeStamp;
    }

    abstract public boolean isError();

    enum Format {
        LOG_ENTRY
    }
}

class LogEntry extends Entry {

    private static String regex = "\\[(.*)]\\s\\[(.*):(.*)]\\s\\[(.*)]\\s\\[(.*)]\\s(.+)";
    private static Pattern pattern = Pattern.compile(regex);

    LogEntry(String line) {
        Matcher matches = pattern.matcher(line);
        if (matches.find()) {
            String group1 = matches.group(1);
            this.timeStamp = LocalDateTime.parse(group1, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZone(ZoneId.systemDefault()).toInstant();
            this.level = matches.group(3);
        }
    }

    public boolean isError() {
        return this.level.equals("error");
    }
}
