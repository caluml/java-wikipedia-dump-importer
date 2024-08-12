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
import java.time.Period;
import java.time.temporal.ChronoUnit;


public class Main {

	private static final String TEXT = "text";
	private static final String TITLE = "title";

	/*
	 * CREATE USER wiki ENCRYPTED PASSWORD 'wiki';
	 * CREATE DATABASE wiki OWNER wiki ENCODING 'UTF-8';
	 * CREATE TABLE pages (
	 *   title varchar NOT NULL PRIMARY KEY,
	 *   text varchar NOT NULL
	 * );
	 */

	public static void main(String[] args) {
		System.setProperty("jdk.xml.totalEntitySizeLimit", "0");
		boolean decompressConcatenated = true;
		int defaultBufferSize = 1024 * 1024 * 64;
		String file = args[0];

		long start = System.currentTimeMillis();
		int count = 0;

		String title = "";
		StringBuilder text = new StringBuilder();

		long length = new File(file).length();

		try (FileInputStream fileInputStream = new FileInputStream(file);
				 BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, defaultBufferSize);
				 BZip2CompressorInputStream compressorInputStream = new BZip2CompressorInputStream(bufferedInputStream, decompressConcatenated)) {

			XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
			xmlInputFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
			xmlInputFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			XMLEventReader reader = xmlInputFactory.createXMLEventReader(compressorInputStream, "UTF-8");

			boolean isTitle = false;
			boolean isText = false;

			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection(args[1], args[2], args[3]);

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

						PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO pages (title, text) VALUES (?, ?) " +
																																							"ON CONFLICT (title) DO UPDATE SET text = ? WHERE pages.title = ?");
						preparedStatement.setString(1, title);
						preparedStatement.setString(2, text.toString());
						preparedStatement.setString(3, text.toString());
						preparedStatement.setString(4, title);
						preparedStatement.execute();

						if (count % 1000 == 0) {
							System.out.print(Instant.now().truncatedTo(ChronoUnit.SECONDS) + ": ");
							System.out.print("ETA: " + getEta(length, compressorInputStream.getCompressedCount(), start) + ": ");
							System.out.print(getPercent(compressorInputStream.getCompressedCount(), length) + "% \t: ");
							System.out.println(title);
						}

						isTitle = false;
						isText = false;
						title = "";
						text = new StringBuilder();
						count++;
					}
				}
			}
		} catch (Exception e) {
			System.err.println("Error processing record " + count + ": " + title + ", " + text);
			e.printStackTrace(System.err);
		} finally {
			System.err.println("Started at  " + Instant.ofEpochMilli(start));
			System.err.println("Finished at " + Instant.now());
			long ms = System.currentTimeMillis() - start;
			long msPerRecord = ms / count;
			System.err.println("Processed " + count + " in " + ms + " ms (" + msPerRecord + " ms/record)");
		}
	}

	private static String getEta(long total,
															 long current,
															 long start) {
		float ms = System.currentTimeMillis() - start;
		float completed = current / (float) total;
		float toGo = 1.0F - completed;

		float msToGo = toGo / (completed / ms);

		Instant etaInstant = Instant.now().plusMillis((long) msToGo);
		return Duration.between(Instant.now(), etaInstant).truncatedTo(ChronoUnit.SECONDS) + " (" + etaInstant.truncatedTo(ChronoUnit.SECONDS) + ")\t";
	}

	private static float getPercent(long current,
																	long length) {
		return (current / (float) length) * 100F;
	}

}
