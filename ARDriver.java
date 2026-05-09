import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ARDriver {

    // Handles quoted fields containing commas (e.g. "Drama, Comedy")
    private static String[] parseCSVLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString().trim());
        return fields.toArray(new String[0]);
    }

    // -------------------------------------------------------------------------
    // Mapper
    // Output key:   type,genre,decade
    // Output value: sum,count,min,max  (sum == rating, count == 1 per record)
    // -------------------------------------------------------------------------
    public static class ARMapper extends Mapper<Object, Text, Text, Text> {

        private final Text outKey   = new Text();
        private final Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = parseCSVLine(line);

            // Skip header
            if (fields.length > 0 && fields[0].equalsIgnoreCase("name")) return;

            // Need at least 8 columns (indices 0-7)
            if (fields.length < 8) return;

            String type       = fields[5].trim();   // col 5
            String genreField = fields[2].trim();   // col 2
            String releasedAt = fields[1].trim();   // col 1
            String imdbField  = fields[7].trim();   // col 7

            // Skip records with any missing required field
            if (type.isEmpty() || genreField.isEmpty() || releasedAt.isEmpty() || imdbField.isEmpty()) return;

            // First genre only (split on comma)
            String genre = genreField.split(",")[0].trim();
            if (genre.isEmpty()) return;

            // Decade from "YYYY-MM-DD"
            String decade;
            try {
                String yearStr = releasedAt.split("-")[0].trim();
                int year = Integer.parseInt(yearStr);
                if (year < 1000 || year > 9999) return;
                decade = (year / 10 * 10) + "s";
            } catch (Exception e) {
                return;
            }

            // Numeric rating from "7.8/10"
            double rating;
            try {
                String ratingStr = imdbField.contains("/")
                        ? imdbField.split("/")[0].trim()
                        : imdbField.trim();
                rating = Double.parseDouble(ratingStr);
            } catch (Exception e) {
                return;
            }

            outKey.set(type + "," + genre + "," + decade);
            // value encodes: sum, count, min, max
            outValue.set(rating + ",1," + rating + "," + rating);
            context.write(outKey, outValue);
        }
    }

    // -------------------------------------------------------------------------
    // Combiner - partial aggregation on each mapper node
    // Same input/output types as Reducer so Hadoop can apply it
    // -------------------------------------------------------------------------
    public static class ARCombiner extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum   = 0.0;
            long   count = 0;
            double min   =  Double.MAX_VALUE;
            double max   = -Double.MAX_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                double s  = Double.parseDouble(parts[0]);
                long   c  = Long.parseLong(parts[1]);
                double mn = Double.parseDouble(parts[2]);
                double mx = Double.parseDouble(parts[3]);
                sum   += s;
                count += c;
                if (mn < min) min = mn;
                if (mx > max) max = mx;
            }

            outValue.set(sum + "," + count + "," + min + "," + max);
            context.write(key, outValue);
        }
    }

    // -------------------------------------------------------------------------
    // Reducer - final aggregation across all partial results
    // Output: Type, Genre, Decade, Count, AverageRating, MinRating, MaxRating
    // -------------------------------------------------------------------------
    public static class ARReducer extends Reducer<Text, Text, NullWritable, Text> {

        private final Text outValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum   = 0.0;
            long   count = 0;
            double min   =  Double.MAX_VALUE;
            double max   = -Double.MAX_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                double s  = Double.parseDouble(parts[0]);
                long   c  = Long.parseLong(parts[1]);
                double mn = Double.parseDouble(parts[2]);
                double mx = Double.parseDouble(parts[3]);
                sum   += s;
                count += c;
                if (mn < min) min = mn;
                if (mx > max) max = mx;
            }

            double avg = sum / count;

            // key format: "type,genre,decade"
            String[] keyParts = key.toString().split(",", 3);
            String type   = keyParts[0];
            String genre  = keyParts[1];
            String decade = keyParts[2];

            outValue.set(String.format(
                "%s, %s, %s, %d, %.2f, %.1f, %.1f",
                type, genre, decade, count, avg, min, max
            ));
            context.write(NullWritable.get(), outValue);
        }
    }

    // -------------------------------------------------------------------------
    // Driver
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ARDriver <input path> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating by Type, Genre, and Decade");

        job.setJarByClass(ARDriver.class);
        job.setMapperClass(ARMapper.class);
        job.setCombinerClass(ARCombiner.class);
        job.setReducerClass(ARReducer.class);

        // Mapper and Combiner output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer final output types
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
