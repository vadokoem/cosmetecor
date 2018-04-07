require 'sequel'
require 'mechanize'
require 'logging'

$log_error = File.new('../log/error_manager.log', 'a')
$info = Logging.logger['manager']
$info.add_appenders(
  Logging.appenders.rolling_file('../log/info_manager.log')
)
$info.level = :info

$error = Logging.logger['manager_error']
$error.add_appenders(
  Logging.appenders.rolling_file('../log/error_manager.log')
)
$error.level = :error
$stderr = $log_error

$db = Sequel.mysql2(:user => 'root', :password => 'skif13', :database=>'db', :max_connections => 4)
Sequel.default_timezone = :utc

table = :radiation_belt

unless $db.table_exists? table
  $db.transaction do
    $db.run('SET storage_engine=MyISAM')
    $db.create_table? table do
      primary_key :id
      Datetime :date, :index => true
      Integer :sensor
      Float :total_belt_index
      Float :inner_belt_index
      Float :outer_belt_index
      String :source, :size => 8, :index => true
    end
    $db.run('SET storage_engine=InnoDB')
  end
end

agent = Mechanize.new
urls = {:"N15" => 'http://satdat.ngdc.noaa.gov/sem/poes/data/belt_indices/noaa15/bi_N15_2013.txt',
        #:"N16" => 'http://satdat.ngdc.noaa.gov/sem/poes/data/belt_indices/noaa16/bi_N16_2013.txt',
        :"N18" => 'http://satdat.ngdc.noaa.gov/sem/poes/data/belt_indices/noaa18/bi_N18_2013.txt',
        :"N19" => 'http://satdat.ngdc.noaa.gov/sem/poes/data/belt_indices/noaa19/bi_N19_2013.txt',
        :"M02" => 'http://satdat.ngdc.noaa.gov/sem/poes/data/belt_indices/metop02/bi_M02_2013.txt'}

count = $db[table].count
after = nil
dw = nil
before = nil
urls.each do |kkk, url_temp|
  dw = $db[:download].where(:text => "radiation_belt_#{kkk}").first
  before = dw[:date]
  url = url_temp if (dw[:date].to_date + 1).year < 2014
  a = url_temp.index('_2013')
  (url = url_temp; url[a + 1..a + 4] = Time.now.year.to_s) unless url
  file = agent.get(url)
  #p [before,url]
  content = file.content
  sputnik = url.split('/')[-1].split('_')[1]
  $db.transaction do
    content.each_line do |line|
      next if line[0] == ':' || line[0] == '#' || line.size < 5
      line = line.split
      date = Time.parse("#{line[0]}.#{line[1]}.#{line[2]}T0:0:0Z")
      key = {:date => date}
      next if key[:date].to_time.to_i <= before.to_time.to_i
      after = date
      key[:sensor] = line[3].to_i
      key[:total_belt_index] = line[4].to_f
      key[:inner_belt_index] = line[5].to_f
      key[:outer_belt_index] = line[7].to_f
      key[:source] = sputnik
      $db[table].insert(key)
    end
  end
  $db[:download].where(:id => dw[:id]).update(:date => after) if after
  $info.info "#{$0} | Add #{$db[table].count - count}. Time change from #{before} to #{after}"
end