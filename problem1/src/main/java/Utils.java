import java.util.regex.Pattern;

public class Utils {
    private Utils() {}

    public static final Pattern WORD_PATTERN = Pattern.compile("^[a-z_\\-]{6,24}$");
    public static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9]+([.,][0-9]+)?$");
    public static final String WORD_SEPARATOR = "[^a-z0-9.,_\\-]+";
    public static final String WHITESPACES = "\\s+";
}
