require 'sequel'
require 'logging'

Dir.chdir $0[0...$0.rindex('/')] if $0.index('/')

if RUBY_PLATFORM.downcase.index("x86_64-linux")
  require 'daemon'
  Daemon.daemonize('daemon.pid', '../log/log_daemon_manager.log') if (ARGV.index('-d') || ARGV.index('-dm'))
end

daemon = true if ARGV.index('-d') || ARGV.index('-dm')
$info = Logging.logger['manager']
$info.add_appenders(
  Logging.appenders.rolling_file('../log/info_manager.log', :age => 'daily', :keep => 3)
)
$info.level = :info

$error = Logging.logger['manager_error']
$error.add_appenders(
  Logging.appenders.rolling_file('../log/error_manager.log', :age => 'daily', :keep => 3)
)
$error.level = :error

$info.info('-' * 15 + Time.now.getgm.to_s + '-' * 15)
$info.info('-' * 15 + 'DAEMON' + '-' * 15) if daemon

$db = Sequel.mysql2(:user => 'root', :password => 'skif13', :database=>'db', :max_connections => 4)

Sequel.default_timezone = :utc

$db.transaction do
  $db.create_table? :download do
    primary_key :id
    String :text
    String :ruby
    Integer :timeout_sec
    Datetime :date
  end
end

$db[:download].insert(:text => 'lightning', :ruby => 'lightning.rb', :timeout_sec => 3600 * 6 ,:date => Time.now.getgm.to_date - 8) if $db[:download].where(:text => 'lightning').first.nil?
$db[:download].insert(:text => 'se_measurements', :ruby => 'se_measurements.rb', :timeout_sec => 3600 * 6, :date => Time.now.getgm.to_date - 360) if $db[:download].where(:text => 'se_measurements').first.nil?
$db[:download].insert(:text => 'usgs', :ruby => 'usgs.rb', :timeout_sec => 3600 ,:date => Time.now.getgm.to_date - 14) if $db[:download].where(:text => 'usgs').first.nil?
$db[:download].insert(:text => 'kam', :ruby => 'kam.rb', :timeout_sec => 3600 ,:date => Time.now.to_date - 360) if $db[:download].where(:text => 'kam').first.nil?
$db[:download].insert(:text => 'geomag', :ruby => 'geomag.rb', :timeout_sec => 3600 * 6 ,:date => Time.now.to_date - 45) if $db[:download].where(:text => 'geomag').first.nil?
$db[:download].insert(:text => 'sunspot', :ruby => 'sunspot.rb', :timeout_sec => 3600 * 6 ,:date => Time.now.to_date - 45) if $db[:download].where(:text => 'sunspot').first.nil?
$db[:download].insert(:text => 'radiation_belt', :ruby => 'radiation_belt.rb', :timeout_sec => 3600 * 6 ,:date => Time.now.to_date - 720) if $db[:download].where(:text => 'radiation_belt').first.nil?
$db[:download].insert(:text => 'hemi', :ruby => 'hemi.rb', :timeout_sec => 3600 ,:date => Time.now.to_date - 3) if $db[:download].where(:text => 'hemi').first.nil?
$db[:download].insert(:text => 'cycles', :ruby => 'cycles.rb', :timeout_sec => 3600*24*7 ,:date => Time.now.to_date - 1) if $db[:download].where(:text => 'cycles').first.nil?
$db[:download].insert(:text => 'gu1_bo1', :ruby => 'gu1_bo1.rb', :timeout_sec => 3600*6 ,:date => Time.now.to_date - 1) if $db[:download].where(:text => 'gu1_bo1').first.nil?

["N15", "N16", "N18", "N19", "M02"].each do |fr|
  $db[:download].insert(:text => "radiation_belt_#{fr}", :ruby => 'empty', :timeout_sec => 3600 * 6 ,:date => Time.now.to_date - 720) if $db[:download].where(:text => "radiation_belt_#{fr}").first.nil?
end

threads = []
db = $db[:download].all
$db.disconnect
db.each do |ag|
  if ag[:ruby] != "empty" && ag[:ruby] != 'hemi.rb' ##hemi die
    threads << Thread.new{
    first_start = 0
      while true
        first_start += 1
        t = Time.now.getgm
        $info.info "Start: #{ag[:ruby]} at #{t}"
        system("ruby #{ag[:ruby]}") unless ag[:ruby] == "empty" 
        if ag[:ruby] == 'sunspot.rb' && first_start == 1
          t_loc = Time.now.getgm
          s_loc = Time.parse("#{t_loc.year}/#{t_loc.month}/#{t_loc.day + 1}T03:10:00Z")
          $info.info "Sleep: #{ag[:ruby]} at #{t_loc} to #{s_loc}"
          sleep(s_loc - t_loc)
        elsif ag[:ruby] == 'se_measurements.rb' && first_start == 1
          t_loc = Time.now.getgm
          s_loc = Time.parse("#{t_loc.year}/#{t_loc.month}/#{t_loc.day + 1}T01:10:00Z")
          $info.info "Sleep: #{ag[:ruby]} at #{t_loc} to #{s_loc}"
          sleep(s_loc - t_loc)
        else
          z = Time.now.getgm - t
          tt = Time.now.getgm
          $info.info "Sleep: #{ag[:ruby]} at #{tt} to #{tt + ag[:timeout_sec] - z}"
          sleep(ag[:timeout_sec] - z) if ag[:timeout_sec] - z > 0
        end
      end
    }
  end
end

threads.each { |aThread|  aThread.join }