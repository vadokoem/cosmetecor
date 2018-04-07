require 'sequel'
require 'fileutils'
require 'dropbox'
Dir.chdir '/srv/ruby-www/content/paint8in1' if $0.index('/')

$db = Sequel.mysql2(:user => 'root', :password => 'skif13', :database=>'db', :max_connections => 4)
Sequel.default_timezone = :utc

tables = eval(ARGV[0])
tables.map!{|a| "se_measurements_#{a}"}

d1 = Time.parse(ARGV[1]).to_date
d2 = Time.parse(ARGV[2]).to_date
date_now = Time.now

tables.each do |table|
  ch_count = $db[:channels_name].where(:code => "#{table}").count * 2
  zwug = 0
  az = []
  while zwug < ch_count
    zwug + 8 > ch_count ? rt = ch_count : rt = zwug + 8
    az << (zwug...rt)
    zwug += 8
  end
  #p az

  db = $db[table.to_sym].select(:date).where{(date >= d1) & (date < d2)}.order(:date)
  (0...ch_count).each do |t|
    db = db.select_append("v#{t}".to_sym)
  end

  srand(Time.now.to_f + rand(10 ** 10))
  random = rand(10 ** 10)
  random_time = Time.now.to_i
  FileUtils.mkdir_p("/srv/ruby-www/temp/mysql", :mode => 777)
  FileUtils.chown 'mysql', 'mysql', "/srv/ruby-www/temp/mysql"
  file_csv = "/srv/ruby-www/temp/mysql/gnuplot_#{random_time}_#{random}.csv"
  db_to_file = db.sql + " INTO OUTFILE '#{file_csv}' FIELDS TERMINATED BY '\\t' ENCLOSED BY '' LINES TERMINATED BY '\\n'"
  db_to_file.sub!("`date`", "date_format(date, '%Y.%m.%dT%H:%i:%S' )")
  $db.fetch(db_to_file).all
  #####file_csv = "/srv/ruby-www/temp/mysql/gnuplot_1506462245_1983982933.csv"

  megaco_name = $db[:channels_name].where(:code => "#{table}").all.map{|a| a[:channel_name_eng]}
  megacoeffs_name = megaco_name.map{|a| a.gsub("~", "=")} + megaco_name.map{|a| a.gsub("~", "\\~")}

  az.each do |dd|
    plot = ""
    eq = {}
    dd.each do |g|
      t = g + 2
      #plot += "set title '#{qwert[g - dd.to_a[0]]}' offset graph 0.0001, -1.1\n"
      plot += "set label 1 center at graph 0.5, graph -0.14 '#{megacoeffs_name[g]}'\n"
      plot += "set label 2 'mV' at graph -0.005, graph 1.02\n"
      plot += "set label 3 'Date' at graph 1.007, graph 0.02\n"
      x = "2017.09.08"
      plot += "set arrow from \"#{x}\",graph 1.0 to \"#{x}\",graph 0.0 lw 3 linecolor rgb 'black'\n"
      eq[:magnitude]= "8.3"
      plot += "set label 4 'M#{eq[:magnitude]}' at '#{x}', graph 1.0525 font \"arialbd,22\" center\n"
      plot += "plot '#{file_csv}' using 1:#{t} with lines linewidth 2 linecolor rgb 'black'\n"
    end
    #p plot
    plot_dir = "/srv/ftp/test/8in1/#{date_now.strftime("%Y.%m.%d")}"
    FileUtils.mkdir_p(plot_dir)
    text = File.open("gnu.dat", 'r:UTF-8', &:read)
    path = "#{plot_dir}/#{table}_#{dd.to_a[0]}-#{dd.to_a[-1]}.png"
    text.sub!("PATH", path)
    text.sub!("PLOTT", plot)
    File.open("gnu.io", 'w:UTF-8') {|f| f.write text}
    system("gnuplot gnu.io")
    while !File.exists? path
      tries += 1
      sleep 0.1 if tries < 50
      (p "no image 8in1"; break) if tries >= 50
    end
    client = Dropbox::Client.new("MUvd7gv_XoAAAAAAAAAAKzIPVjDCl0wkuqo9V1sZLmlTvs56mqumyChr_EmpDzed")
    dropbox_path = "/8in1/#{date_now.strftime("%Y.%m.%d")}/#{table}_#{dd.to_a[0]}-#{dd.to_a[-1]}.png"
    response = client.upload(dropbox_path, File.open(path, 'r', &:read))
    #p response
  end

  FileUtils.rm file_csv
end