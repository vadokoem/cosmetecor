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

dw = $db[:download].where(:text => 'lightning').first
table = :lightning

unless $db.table_exists? table
  $db.create_table? table do
    Datetime :date, :index => true
    Float :coord_x, :index => true
    Float :coord_y, :index => true
  end
end
count = $db[table].count

agent = Mechanize.new
page = agent.get('http://flash3.ess.washington.edu/USGS/AVO/archive/')

date = dw[:date].strftime("%Y%m%d")
keys = [:date, :coord_x, :coord_y]
page.links.reverse.each do |link|
  if link.text.index(date)
    break
  else
    kmls = link.click
    all = []
    kmls.links.each do |kml|
      next unless kml.text.index('.kml')
      k = kml.click.content
      xml = XmlSimple::xml_in(k)
      xml = xml['Document'].first['Folder'].first['Folder']
      xml.each do |x|
        if x['Placemark']
          x['Placemark'].each do |y|
            coord = []
            coord << (Time.parse y['name'].first)
            co = y['Point'].first['coordinates'].first.split(',')
            coord << co[0]
            coord << co[1]
            all << coord
          end
        end
      end
    end
    # logger.info "#{$0} | #{all.size}"
    # logger.info "#{$0} | #{p all}"
    all = all.uniq
    # logger.info "#{$0} | #{all.size}"
    # logger.info "#{$0} | #{p all}"
    $db[table].import(keys, all)
  end
end
after = Time.parse(page.links[-1].text).to_date
$info.info "#{$0} | Add #{$db[table].count - count}. Time change from #{dw[:date]} to #{after}"
$db[:download].where(:id => dw[:id]).update(:date => after) if after