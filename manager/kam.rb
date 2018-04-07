require 'sequel'
require 'mechanize'
require 'xmlsimple'
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

dw = $db[:download].where(:text => 'kam').first
table = :earth_kam

unless $db.table_exists? table
  $db.create_table? table do
    primary_key :id
    Datetime :date, :index => true
    Float :magnitude, :index => true
    Float :latitude
    Float :longitude
    Float :depth
  end
end
count = $db[table].count

agent = Mechanize.new
page = agent.get('http://emsd.ru/ts/all.php')

date = dw[:date] - 31 * 24 * 3600
$db[table].where("date >= ?", date).delete
form = page.form('find')
form.y1 = date.year.to_s
form.m1 = date.month.to_s
  form.m1 = '0' + form.m1 if form.m1.size == 1
form.d1 = date.day.to_s
  form.d1 = '0' + form.d1 if form.d1.size == 1
form.ks1 = '9.5'
form.ks2 = '19.9'
form.checkbox_with(:name => 'cbks').check
form.magnname = 'ks'
page = form.submit(form.button_with(:name => 'katxml'))
after = nil
$db.transaction do
  xml = XmlSimple::xml_in(page.content)
  xml = xml['Worksheet'].first['Table'].first['Row']
  xml.shift
  while xml[0]['Cell'].first['Data'].first['content'].nil?
    xml.shift
  end
  xml.shift
  xml.each do |a|
    z = []; key = {}
    a['Cell'].each do |c|
      z << c['Data'].first['content']
    end
    z[-1].to_f >= 4 ? key[:magnitude] = z[-1].to_f : next
    key[:ks] = z[-2].to_f
    key[:date] = Time.parse("#{z[1]}.#{z[2]}.#{z[3]}T#{z[4]}:#{z[5]}:#{z[6]}Z")
    next if key[:date].to_i <= date.to_i
    key[:depth] = z[-1].to_f
    key[:latitude] = z[8]
    key[:longitude] = z[9]
    key[:depth] = z[11]
    after = key[:date]
    $db[table].insert(key)
  end
end

$info.info "#{$0} | Add #{$db[table].count - count}. Time change from #{dw[:date]} to #{after}"
$db[:download].where(:id => dw[:id]).update(:date => after) if after