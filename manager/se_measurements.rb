require 'sequel'
require 'seven_zip_ruby'
require 'logging'
require 'fileutils'

$log_error = File.new('../log/error_manager.log', 'a')
$info = Logging.logger['manager_info']
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

$db.create_table? :error_read_se do
  primary_key :id
  Datetime :date
  String :path
  String :key
end

$dirs = {:'desp' => '/srv/ftp/s1-desp',
         :'desp-uz' => '/srv/ftp/s1-desp-uz',
         :'s1-elisovo' => '/srv/ftp/s1-elisovo',
         :'ifpet' => '/srv/ftp/s2-imfset',
         :'s3-okean' => '/srv/ftp/s3-imfset',
         :'s4-esso' => '/srv/ftp/s4-imfset',
         :'s5-altai' => '/srv/ftp/s5-imfset',
         :'s6-chieti' => '/srv/ftp/s6-imfset',
         :'s7-okean' => '/srv/ftp/s7-imfset',
         :'s8-imfset' => '/srv/ftp/s8-imfset',
         :'s9-imfset' => '/srv/ftp/s9-imfset',
         :'s12-imfset-fiji' => '/srv/ftp/s12-imfset-fiji'
         #:'s10-imfset' => '/srv/ftp/s10-imfset'#,
         #:'s11-imfset' => '/srv/ftp/s11-imfset'
         }

$back_dirs = {:'desp' => 's1-DESP-PK',
              :'desp-uz' => 's1-DESP-UZ',
              :'s1-elisovo' => 'S1-IMFSET',
              :'ifpet' => 's2-imfset',
              :'s3-okean' => 's3-imfset',
              :'s4-esso' => 's4-esso',
              :'s5-altai' => 's5-altai',
              :'s6-chieti' => 's6-chieti',
              :'s7-okean' => 's7-imfset',
              :'s8-imfset' => 's8-imfset',
              :'s9-imfset' => 's9-imfset',
              :'s12-imfset-fiji' => 's12-imfset-fiji'
              #:'s10-imfset' => 's10-imfset'#,
              #:'s11-imfset' => 's11-imfset'
              }


unless $db.table_exists? :se_download
  t = $db[:download].where(:text => 'se_measurements').first
  $db.transaction do
    $db.create_table? :se_download do
      primary_key :id
      String :text
      Date :date
    end
    $dirs.each do |k, v|
      $db[:se_download].insert(:text => k.to_s, :date => t[:date])
    end
  end
end

def read
  $dirs.each do |k, v|
    $info.info "#{$0} | Start reading directory #{v}"
    read_error k
    read_dir v, k
    $info.info "#{$0} | End reading directory #{v}"
  end
  #--=-=-=--==-=
  $dirs.each do |key, dir|
    next unless Dir.exists? dir
    filenames = Dir.entries(dir).select {|f| !File.directory? f}
    if filenames.size > 0
      filenames.sort.each do |file|
        path = dir + '/' + file
        (FileUtils.remove(path); next) if file[-4] == 's' || file[-4..-1] == '.log'
        after = Time.parse(file[file.rindex('-') + 1...file.rindex('.')]).to_date
        dj = $back_dirs[key]
        year = after.strftime("%Y")
        fname = path[path.rindex("/") + 1..-1]
        dname = path.dup
        dname["/ftp/"] = "/ftp/archive/"
        FileUtils.mkdir("/ftp/archive/#{year}") unless Dir.exists?("/ftp/archive/#{year}")
        FileUtils.mkdir("/ftp/archive/#{year}/#{dj}_#{year}") unless Dir.exists?("/mnt/archive/#{year}/#{dj}_#{year}")
        FileUtils.mv(path, "/ftp/archive/#{year}/#{dj}_#{year}/#{fname}") ##cp
        #FileUtils.mv(path, dname) # copy to public archive ####old
        #p [path, "/mnt/gdr/#{year}/#{dj}_#{year}/#{fname}", dname]
        #-=-=--=-
      end
    end
  end
  # $dirs.each do |key, didi|# clear public archive ####old
    # dir = didi.dup
    # dir["/ftp/"] = "/ftp_2/archive/"
    # next unless Dir.exists? dir
    # filenames = Dir.entries(dir).select{|f| !File.directory? f}.sort!
    # while filenames.size > 180
      # path = dir + '/' + filenames.shift
      # FileUtils.remove(path);
    # end
  # end
  #system 'ruby se_max.rb'
end

def read_error key
  files_read = 0;
  filenames = $db[:error_read_se].where(:key => key.to_s).all
  $db[:error_read_se].where(:key => key.to_s).delete
  if filenames.size > 0
    dead_files = []
    filenames.each do |zzz|
      path = zzz[:path]
      files_read += 1
      state = nil
      if '.7z' == File.extname(path).downcase
        state = read_7z path
      elsif '.log' == File.extname(path).downcase
        state = read_file path
      end
      (dead_files << path) unless state
    end
    dead_files.each do |f|
      t = Time.parse(path[path.rindex('-') + 1...path.rindex('.')] + 'Z')
      $db[:error_read_se].insert(:path => f, :date => t, :key => key)
      File.delete f
    end
  end
  #$logger.info "#{$0} | File read: #{files_read}."
end

def recheck arh
  date1 = Time.parse(arh[arh.rindex('-') + 1...arh.rindex('.')]).to_date
  date2 = date1 + 1
  date1 = date1.to_datetime.to_time.getgm
  date2 = date2.to_datetime.to_time.getgm
  a2 = arh.rindex('-')
  a1 = arh.rindex('/')
  table = "se_measurements_#{arh[a1 + 1...a2]}".downcase.to_sym
  c1 = $db[table].where{date > date1}.where{date <= date2}.count
  return false if c1 == 0
  state = true
  big_data = nil
  path = nil
  File.open(arh, "rb") do |file|
    begin
      SevenZipRuby::Reader.open(file) do |szr|
        files = szr.entries.select(&:file? )
        files.each do |entry|
          path = entry.path
          big_data = szr.extract_data(entry)
        end
      end
    rescue StandardError => ex
      state = false
    end
  end
  if state
    c2 = 0
    big_data.each_line {c2 += 1}
    if (c2 > c1 + 2) || (c2 < c1 - 2)
      $db[table].where{date > date1}.where{date <= date2}.delete
      read_file path, big_data
      return true
    end
  end
  return false
end

def read_dir dir, key
  files_read = 0; after = nil; last_good_after = nil
  return unless Dir.exists? dir
  filenames = Dir.entries(dir).select {|f| !File.directory? f}
  if filenames.size > 0
    dead_files = []
    filenames.sort.each do |file|
      next if file[-4] == 's'
      path = dir + '/' + file
      after = Time.parse(file[file.rindex('-') + 1...file.rindex('.')]).to_date
      a2 = path.rindex('-')
      a1 = path.rindex('/')
      table = "se_measurements_#{path[a1 + 1...a2]}".downcase.to_sym
      next if ($db.table_exists? table) && (recheck path) # if after >= $db[:se_download].where(:text => key.to_s).first[:date].to_date - 3 && after <= $db[:se_download].where(:text => key.to_s).first[:date].to_date
      #next if $db[:se_download].where(:text => key.to_s).first && after <= $db[:se_download].where(:text => key.to_s).first[:date].to_date
      files_read += 1
      state = nil
      if '.7z' == File.extname(file).downcase
        state = read_7z path
      elsif '.log' == File.extname(file).downcase
        state = read_file path
      end
      last_good_after = Time.parse(path[path.rindex('-') + 1...path.rindex('.')] + 'Z') if state
      dead_files << path unless state
    end
    $db[:se_download].where(:text => key.to_s).update(:date => last_good_after.to_date) if last_good_after
    dead_files.each do |f|
      t = Time.parse(f[f.rindex('-') + 1...f.rindex('.')] + 'Z')
      $db[:error_read_se].insert(:path => f, :date => t, :key => key.to_s) if last_good_after && t < last_good_after
      File.delete f
    end
  end
  $info.info "#{$0} | File read: #{files_read}."
end

def read_7z arh
  state = true
  # file = nil
  # date = nil
  File.open(arh, "rb") do |file|
    begin
      SevenZipRuby::Reader.open(file) do |szr|
        files = szr.entries.select(&:file? )
        files.each do |entry|
          # data = szr.extract_data(entry)
          # date = read_file_ram entry.path, data
          # file = entry.path
          data = szr.extract_data(entry)
          read_file arh, data
          #file = entry.path
        end
      end
    rescue StandardError => ex
      state = false
    end
  end
  state
end

# def read_file_ram file, data
  # a2 = file.rindex('-')
  # ind = file[0...a2]
  # table = "se_measurements_#{ind}".downcase.to_sym
  # unless $db.table_exists? table
    # $db.transaction do
      # $db.create_table? table do
        # primary_key :id
        # Datetime :date, :index => true
      # end
      # (0..31).each do |a|
        # $db.alter_table table do
          # add_column :"v#{a}", Float
        # end
      # end
    # end
  # end
  # bfg = []
  # sum = Array.new(32, 0) ##1_average
  # dt = nil ##1_average
  # data.each_line do |line|
    # line = line.split
    # dt = Time.parse(line[0]).to_date if dt.nil? ##1_average
    # date = Time.parse(line[0] + 'T' + line[1] + 'Z')
    # key = []
    # key[0] = date
    # (0..31).each do |a|
      # key[a + 1] = line[2 + a]
      # sum[a] += line[2 + a].to_f ##1_average
    # end
    # bfg << key
  # end
  # keys = [:date]
  # (0..31).each do |a|
    # keys << :"v#{a}"
  # end
  # FileUtils.mkdir_p("/srv/ftp_2/1_day_Average") ##1_average
  # fwr = open("/srv/ftp_2/1_day_Average/#{table}.txt", 'a') ##1_average
  # fwr.write(dt.strftime("%Y.%m.%d")) ##1_average
  # sum.each do |s|
    # fwr.write("\t#{s/86400.0}") ##1_average
  # end
  # fwr.write("\n")
  # count = $db[table].count
  # $db[table].import(keys, bfg, :commit_every => 256)
  # bfg[0][0]
# end

def read_file file, data=nil
  a1 = file.rindex('/')
  a2 = file.rindex('-')
  ind = file[a1 + 1...a2]
  table = "se_measurements_#{ind}".downcase.to_sym
  ch_nums = $db[:channels_name].where(:code => "#{table}").count * 2
  unless $db.table_exists? table
    $db.transaction do
      $db.create_table? table do
        primary_key :id
        Time :date, :index => true
      end
      (0..ch_nums).each do |a|
        $db.alter_table table do
          add_column :"v#{a}", Float
        end
      end
    end
  end
  data = open(file, 'r', &:read) if data == nil
  bfg = []
  # sum = Array.new(ch_nums, 0) ##1_average
  # dt = nil ##1_average
  file.each_line do |line|
    line = line.split
    dt = Time.parse(line[0]).to_date if dt.nil? ##1_average
    date = Time.parse(line[0] + 'T' + line[1] + 'Z')
    key = []
    key[0] = date
    (0..ch_nums).each do |a|
      key[a + 1] = line[2 + a]
      #sum[a] += line[2 + a] ##1_average
    end
    bfg << key
  end
  keys = [:date]
  (0..ch_nums).each do |a|
    keys << :"v#{a}"
  end
  # FileUtils.mkdir_p("/srv/ftp/1_day_Average") ##1_average
  # fwr = open("/srv/ftp/1_day_Average/#{table}.txt", 'a') ##1_average
  # fwr.write(dt.strftime("%Y.%m.%d")) ##1_average
  # sum.each do |s|
    # fwr.write("\t#{s/86400.0}") ##1_average
  # end
  # fwr.write("\n")
  $db[table].import(keys, bfg, :commit_every => 256)
  bfg[0][0]
end

read