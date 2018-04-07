require 'sequel'
require 'logging'
require 'net/ftp'

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

dw = $db[:download].where(:text => 'hemi').first
table = :hemispheric_power_polar

unless $db.table_exists? table
  $db.create_table? table do
    primary_key :id
    Boolean :south, :index => true
    Time :date, :index => true
    Float :power
  end
end

$ftp = nil
def ftp_connect
  $ftp = Net::FTP.new('ftp.swpc.noaa.gov')
  $ftp.passive = true
  $ftp.login('anonymous', 'admin@cosmetecor.org')
  #p "Logon"
end
ftp_connect

content = $ftp.gettextfile '/pub/lists/hpi/pwr_1day', nil
before = dw[:date]

n = 0
key = {}
keys = [:south, :date, :power]
data = []
after = nil
$db.transaction do
  content.each_line do |line|
    line = line.split
    date = Time.parse(line[0] + 'T' + line[1] + 'Z')
    next if date.to_i <= before.to_i
    after = date if after.nil? || after.to_i < date.to_i
    key = []
    key << (line[3] == '(S)' ? true : false)
    key << date
    key << line[5]
    data << key
  end
end
data.sort!{|x, y| y[1] <=> x[1]}
count = $db[table].count
$db[table].import(keys, data)

$info.info "#{$0} | Add #{$db[table].count - count}. Time change from #{dw[:date]} to #{after}"
$db[:download].where(:id => dw[:id]).update(:date => after) if data.size > 0