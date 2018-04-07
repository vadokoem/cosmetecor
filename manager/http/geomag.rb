require 'sequel'
require 'mechanize'
require 'logging'

$log_error = File.new('../log/error_manager.log', 'a')
$info = Logging.logger['manager']
$info.add_appenders(
  Logging.appenders.rolling_file('../log/info_manager.log')
)
$info.level = :info

$error = Logging.logger['manager']
$error.add_appenders(
  Logging.appenders.rolling_file($log_error)
)
$error.level = :error
$stderr = $log_error


$db = Sequel.mysql2(:user => 'root', :password => 'skif13', :database=>'db', :max_connections => 4)
Sequel.default_timezone = :utc

dw = $db[:download].where(:text => 'geomag').first
table = :geomagn_ap

unless $db.table_exists? table
  $db.create_table? table do
    primary_key :id
    Date :date, :index => true
    Integer :middle_latitude
    Integer :high_latitude
    Integer :estimated
  end
end
count = $db[table].count

agent = Mechanize.new
file = agent.get('ftp://ftp.swpc.noaa.gov/pub/indices/DGD.txt')
date = dw[:date]
content = file.content

after = nil
$db.transaction do
  content.each_line do |line|
    next if line[0] == ':' || line[0] == '#'
    line = line.split
    next if line[3] == '-1'
    key = {:date => Date.parse("#{line[0]}.#{line[1]}.#{line[2]}")}
    next if key[:date].to_date <= date.to_date
    after = key[:date]
    key[:middle_latitude] = line[3]
    key[:high_latitude] = line[12]
    key[:estimated] = line[21]
    $db[table].insert(key)
  end
end

$info.info "#{$0} | Add #{$db[table].count - count}. Time change from #{dw[:date]} to #{after}"
$db[:download].where(:id => dw[:id]).update(:date => after) if after