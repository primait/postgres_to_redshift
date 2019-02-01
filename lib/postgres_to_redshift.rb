require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk'
require 'zlib'
require 'tempfile'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri
  end

  attr_reader :source_connection, :target_connection, :s3

  def self.update_tables(filtered_tables: [])
    update_tables = PostgresToRedshift.new(filtered_tables: filtered_tables)

    update_tables.tables.each do |table|
      target_connection.exec("CREATE TABLE IF NOT EXISTS #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      update_tables.copy_table(table)

      update_tables.import_table(table)
    end
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def self.schema
    ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_SCHEMA')
  end

  def initialize(filtered_tables: [])
    @filtered_tables = filtered_tables
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW')").map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      next if filtered_tables.length > 0 and not filtered_tables.include?(table.name)
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= Aws::S3::Client.new
  end

  def bucket
    @bucket ||= Aws::S3::Bucket.new(ENV['S3_DATABASE_EXPORT_BUCKET'], [client: s3])
  end

  def prefix
    @prefix ||= ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_TABLE_PREFIX')
  end

  def copy_table(table)
    tmpfile = Tempfile.new("psql2rs")
    zip = Zlib::GzipWriter.new(tmpfile)
    bucket.objects(prefix: "export/#{table.target_table_name}.psv.gz").batch_delete!
    begin
      puts "Downloading #{table}"
      copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO STDOUT WITH DELIMITER '|'"

      source_connection.copy_data(copy_command) do
        while row = source_connection.get_copy_data
          zip.write(row)
        end
      end
      zip.finish
      tmpfile.rewind
      upload_table(table, tmpfile.path)
      source_connection.reset
    ensure
      zip.close unless zip.closed?
      tmpfile.unlink
    end
  end

  def upload_table(table, source_file_name)
    puts "Uploading #{table.target_table_name}"
    s3_res = Aws::S3::Resource.new
    obj = s3_res.bucket(ENV['S3_DATABASE_EXPORT_BUCKET']).object("data/#{table.target_table_name}/#{table.target_table_name}.psv.gz.1")
    obj.upload_file(source_file_name)
    # s3.put_object({body: source_file_name, bucket: ENV['S3_DATABASE_EXPORT_BUCKET'], key: })
  end

  def import_table(table)
    retries ||= 0

    puts "Importing #{table.target_table_name} (deleting it first)"
    schema = self.class.schema

    target_connection.exec("BEGIN;")

    target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{prefix}_#{table.target_table_name}")

    # target_connection.exec("ALTER TABLE #{schema}.#{target_connection.quote_ident("#{prefix}_#{table.target_table_name}")} RENAME TO #{"#{prefix}_#{table.target_table_name}"}_updating")

    puts table.columns_for_create
    puts sort_keys()

    target_connection.exec("CREATE TABLE #{schema}.#{target_connection.quote_ident("#{prefix}_#{table.target_table_name}")} (#{table.columns_for_create}) #{sort_keys()}")

    target_connection.exec("COPY #{schema}.#{target_connection.quote_ident("#{prefix}_#{table.target_table_name}")} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/data/#{table.target_table_name}/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_iam_role=arn:aws:iam::001575623345:role/redshift-spectrum-role-dev' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

    target_connection.exec("COMMIT;")

  rescue PG::UnableToSend
    retry if (retries += 1) < 3
  end

  def sort_keys()
    "INTERLEAVED SORTKEY(#{ENV['POSTGRES_TO_REDSHIFT_SORT_KEYS']})"
  end

  # def inject_keys()
  #
  # end

  private

  attr_reader :filtered_tables
end
