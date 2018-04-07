require 'sequel'
require 'mechanize'
require 'csv'
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

dw = $db[:download].where(:text => 'usgs').first
table = :earth_usgs

unless $db.table_exists? table
  $db.create_table? table do
    primary_key :id
    Datetime :date, :index => true
    Datetime :solar_time
    Float :magnitude, :index => true
    Float :latitude
    Float :longitude
    Float :depth
    String :place
  end
end

def solar_time_diff longitude
  t = (longitude/15.0).divmod 1
  s = (t[1] * 60).divmod 1
  h = (s[1] * 60).divmod 1
  goal = t[0] * 3600 + s[0] * 60 + h[0]
  #p [longitude, goal, goal/3600.0]
  goal
end

agent = Mechanize.new
file = agent.get('http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.csv')
#file = agent.get('http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.csv')
date = dw[:date] - 3600*24
count = $db[table].count
$db[table].where("date >= ?", date).delete

n = 0
key = {}
keys = [:date, :latitude, :longitude, :depth, :magnitude, :place, :solar_time]
data = []
after = nil
CSV.parse(file.content) do |arr|
  array = []
  n += 1
  next if n < 2  
  mg = arr[4].gsub(',','.').to_f
  next if mg < 4
  array << Time.parse(arr[0])
  next if array[0].to_i < date.to_i
  after = array[0] if after.nil? || after.to_i < array[0].to_i
  array << arr[1].gsub(',','.').to_f
  array << arr[2].gsub(',','.').to_f    
  array << arr[3].gsub(',','.').to_f 
  array << mg
  array << arr[13]
  array << (array[0] + solar_time_diff(array[2]))
  data << array
end

#$db[table].where("date >= ?", data[-1][0]).delete

data.sort!{|x, y| x[1] <=> y[1]}

$db[table].import(keys, data)

$info.info "#{$0} | Add #{$db[table].count - count}. Time change from #{dw[:date]} to #{after}"
$db[:download].where(:id => dw[:id]).update(:date => after) if after