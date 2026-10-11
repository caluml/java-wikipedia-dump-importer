package wikidumpimporter;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

	private static final String TEXT = "text";
	private static final String TITLE = "title";


	// The "Poison Pill" signals the consumer thread that parsing is entirely finished
	private static final WikiPage POISON_PILL = new WikiPage(null, null);

	/*
	 * https://github.com/caluml/java-wikipedia-dump-importer/
	 *
	 * CREATE USER wiki ENCRYPTED PASSWORD 'wiki';
	 * CREATE DATABASE wiki OWNER wiki ENCODING 'UTF-8';
	 * CREATE TABLE pages (
	 *   title varchar NOT NULL PRIMARY KEY,
	 *   text varchar NOT NULL
	 * );
	 *
	 * java -jar wikidump-importer.jar jdbc://postgres/db username password [batchsize]
	 */

	public static void main(String[] args) {
		System.setProperty("jdk.xml.totalEntitySizeLimit", "0");
		boolean decompressConcatenated = true;
		int defaultBufferSize = 1024 * 1024 * 8;
		String file = args[0];

		int batchSize = args.length < 5 || args[4] == null ? 1000 : Integer.parseInt(args[4]);

		System.out.println("File      : " + file);
		System.out.println("Batchsize : " + batchSize);

		long start = System.currentTimeMillis();
		long length = new File(file).length();
		AtomicInteger count = new AtomicInteger(0);

		String sql = "INSERT INTO pages (title, text) VALUES (?, ?) " +
								 "ON CONFLICT (title) DO UPDATE SET text = EXCLUDED.text";

		// Bounded queue prevents OOM if parsing is way faster than DB writes
		BlockingQueue<WikiPage> queue = new ArrayBlockingQueue<>(batchSize * 2);

		Thread dbConsumerThread = new Thread(() -> {
			try {
				Class.forName("org.postgresql.Driver");
				try (Connection connection = DriverManager.getConnection(args[1], args[2], args[3]);
						 PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

					connection.setAutoCommit(false);
					try (PreparedStatement setupStmt = connection.prepareStatement("SET synchronous_commit = off;")) {
						setupStmt.execute();
					}

					int localBatchCount = 0;

					while (true) {
						WikiPage page = queue.take();

						if (page == POISON_PILL) {
							preparedStatement.executeBatch();
							connection.commit();
							break;
						}

						preparedStatement.setString(1, page.title);
						preparedStatement.setString(2, page.text);
						preparedStatement.addBatch();
						localBatchCount++;

						if (localBatchCount % batchSize == 0) {
							preparedStatement.executeBatch();
							connection.commit();
						}
					}
				}
			} catch (Exception e) {
				System.err.println("Database Consumer Thread failed!");
				e.printStackTrace(System.err);
				System.exit(1);
			}
		});
		dbConsumerThread.start();


		String title = "";
		StringBuilder text = new StringBuilder();

		try (FileInputStream fileInputStream = new FileInputStream(file);
				 BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, defaultBufferSize);
				 BZip2CompressorInputStream compressorInputStream = new BZip2CompressorInputStream(bufferedInputStream, decompressConcatenated)) {

			XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
			xmlInputFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
			xmlInputFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			XMLEventReader reader = xmlInputFactory.createXMLEventReader(compressorInputStream, "UTF-8");

			boolean isTitle = false;
			boolean isText = false;

			while (reader.hasNext()) {
				XMLEvent event = reader.nextEvent();

				if (event.isStartElement()) {
					StartElement startElement = event.asStartElement();
					String localPart = startElement.getName().getLocalPart();
					if (localPart.equals(TITLE)) isTitle = true;
					if (localPart.equals(TEXT)) isText = true;
				}

				if (event.isCharacters()) {
					Characters characters = event.asCharacters();
					if (isTitle) {
						title = characters.getData();
						isTitle = false;
					}
					if (isText) {
						if (characters.getData() != null) {
							text.append(characters.getData());
						}
					}
				}

				if (event.isEndElement()) {
					EndElement endElement = event.asEndElement();
					if (endElement.getName().getLocalPart().equals(TEXT)) {

						// Pass the parsed page to the queue (blocks if queue is full)
						queue.put(new WikiPage(title, text.toString()));

						int currentCount = count.incrementAndGet();

						if (currentCount % batchSize == 0) {
							System.out.print(Instant.now().truncatedTo(ChronoUnit.SECONDS) + ": ");
							System.out.print("ETA: " + getEta(length, compressorInputStream.getCompressedCount(), start, currentCount) + ": ");
							System.out.print(getPercent(compressorInputStream.getCompressedCount(), length) + "% \t: ");
							System.out.println(title);
						}

						isTitle = false;
						isText = false;
						title = "";

						// Prevent memory leak for giant articles
						if (text.capacity() > 1024 * 1024) {
							text = new StringBuilder(1024 * 64);
						} else {
							text.setLength(0);
						}
					}
				}
			}

			// Parsing complete. Tell the consumer to finish up and wait for it.
			queue.put(POISON_PILL);
			dbConsumerThread.join();

		} catch (Exception e) {
			System.err.println("Error processing record " + count.get() + ": " + title);
			e.printStackTrace(System.err);
		} finally {
			System.err.println("Started at  " + Instant.ofEpochMilli(start));
			System.err.println("Finished at " + Instant.now());
			long ms = System.currentTimeMillis() - start;
			int totalProcessed = count.get();
			long msPerRecord = totalProcessed > 0 ? ms / totalProcessed : 0;
			System.err.println("Processed " + totalProcessed + " in " + ms + " ms (" + msPerRecord + " ms/record)");
		}
	}

	private static String getEta(long totalBytes,
															 long currentBytes,
															 long start,
															 int recordsProcessed) {
		if (currentBytes == 0 || recordsProcessed == 0) return "Calculating ETA...\t";

		long elapsedMs = System.currentTimeMillis() - start;
		long msToGo = (long) ((double) elapsedMs * (totalBytes - currentBytes) / currentBytes);

		Duration remaining = Duration.ofMillis(msToGo);
		Instant etaInstant = Instant.now().plusMillis(msToGo);

		// Calculate current speed
		double msPerRecord = (double) elapsedMs / recordsProcessed;

		return String.format("%s @ %s [%.2f ms/record]\t",
			remaining.truncatedTo(ChronoUnit.SECONDS),
			etaInstant.truncatedTo(ChronoUnit.SECONDS),
			msPerRecord);
	}

	private static float getPercent(long current,
																	long length) {
		return (current / (float) length) * 100F;
	}


	// A simple container to pass data between the parsing thread and DB thread
	private static class WikiPage {

		final String title;
		final String text;

		WikiPage(String title,
						 String text) {
			this.title = title;
			this.text = text;
		}
	}
}
