# AVRO Dependencies
import avro.schema as schema
from avro.datafile import DataFileReader, DataFileWriter, DataFileException, VALID_CODECS, SCHEMA_KEY
from avro.io import DatumReader
from avro import io as avro_io

# CSV dependencies
import csv

class VivaAvroBinReader(DataFileReader):
	def __init__(self, reader, datum_reader):
		"""
			Initializes a new binary avro file

			Args:
				- reader: Open file to read from
				- datum_reader: Avro datum reader
		"""
		self._reader = reader
		self._raw_decoder = avro_io.BinaryDecoder(reader)
		self._datum_decoder = None # Maybe reset at every block
		self._datum_reader = datum_reader

		# Read the header (magic, meta, sync)
		self._read_header()

		# Ensure codec is valid
		avro_codec_raw = self.GetMeta('avro.codec')
		
		if avro_codec_raw is None:
			self.codec = "null"
		else:
			self.codec = avro_codec_raw.decode('utf-8')

		if self.codec not in VALID_CODECS:
			raise DataFileException('Unknown codec: %s.' % self.codec)
		
		self._file_length = self._GetInputFileLength()

		# Getting ready to read
		self._block_count = 0
		self.datum_reader.writer_schema = (
			schema.Parse(self.GetMeta(SCHEMA_KEY).decode('utf-8'))
		)




reader = VivaAvroBinReader(open('toParse.avro', "rb"), DatumReader())

fileParsed = open('viva_avro_parsed.csv', 'w')
csv_writer = csv.writer(fileParsed)

isHeadersWritten = False

for data in reader:
	if isHeadersWritten == False:
		headers = data.keys()
		csv_writer.writerow(headers)
		isHeadersWritten = True
	csv_writer.writerow(data.values())

fileParsed.close()
reader.close()