require 'sequel'

$db = Sequel.mysql2(:user => 'root', :password => 'skif13', :database=>'db', :max_connections => 4)
Sequel.default_timezone = :utc

#-----------=-=-=-=--=- GU1
def calc_gu1
  unless $db.table_exists? :gu1
    $db.create_table? :gu1 do
      primary_key :id
      Date :date, :index => true
      Float :coeffs_4_5
      Float :coeffs_5_6
      Float :coeffs_6_7
    end
    usgs_date_rigth = $db[:earth_usgs].order(Sequel.desc(:date)).limit(1).all.first[:date].to_date
    usgs_date_left = $db[:earth_usgs].order(Sequel.asc(:date)).limit(1).all.first[:date].to_date
    rg = usgs_date_left + 1
    while usgs_date_left < usgs_date_rigth
      quakes = $db[:earth_usgs].order(Sequel.asc(:date)).
                                where{(date >= usgs_date_left) & (date < rg)}
      gu1 = []
      [[4, 5], [5, 6], [6, 7]].each do |mg|
        craters = quakes.where{(magnitude >= mg[0].to_i) & (magnitude < mg[1].to_i)}
        count = craters.count

        max_e = 0
        if count > 0
          qqq = craters.all.map{|s| 10**(1 + 1 * s[:magnitude])}
          max_e = qqq.max{|a, b| a <=> b}
          #p count, max_e
        end
        gu1 << (count * max_e)
      end
      $db[:gu1].insert({:date => usgs_date_left,
                        :coeffs_4_5 => gu1[0],
                        :coeffs_5_6 => gu1[1],
                        :coeffs_6_7 => gu1[2]})

      usgs_date_left += 1
      rg = usgs_date_left + 1
    end
  end

  gu1_date = $db[:gu1].order(Sequel.desc(:date)).limit(1).all.first[:date].to_date
  usgs_date_rigth = $db[:earth_usgs].order(Sequel.desc(:date)).limit(1).all.first[:date].to_date
  gu1_date += 1
  if gu1_date < usgs_date_rigth - 1
    rg = gu1_date + 1
    while gu1_date < usgs_date_rigth - 1
      quakes = $db[:earth_usgs].order(Sequel.asc(:date)).
                                where{(date >= gu1_date) & (date < rg)}
      gu1 = []
      [[4, 5], [5, 6], [6, 7]].each do |mg|
        craters = quakes.where{(magnitude >= mg[0].to_i) & (magnitude < mg[1].to_i)}
        count = craters.count

        max_e = 0
        if count > 0
          qqq = craters.all.map{|s| 10**(1 + 1 * s[:magnitude])}
          max_e = qqq.max{|a, b| a <=> b}
          #p count, max_e
        end
        gu1 << (count * max_e)
      end
      $db[:gu1].insert({:date => gu1_date,
                        :coeffs_4_5 => gu1[0],
                        :coeffs_5_6 => gu1[1],
                        :coeffs_6_7 => gu1[2]})
      gu1_date += 1
      rg = gu1_date + 1
    end
  end
end
#-----------=-=-=-=--=- GU1-=-=-=-=-=- END

#-----------=-=-=-=--=- BO1
def calc_bo1
  unless $db.table_exists? :bo1
    $db.create_table? :bo1 do
      primary_key :id
      Date :date, :index => true
      Float :coeffs_4_5
      Float :coeffs_5_6
      Float :coeffs_6_7
    end
    usgs_date_rigth = $db[:earth_usgs].order(Sequel.desc(:date)).limit(1).all.first[:date].to_date
    usgs_date_left = $db[:earth_usgs].order(Sequel.asc(:date)).limit(1).all.first[:date].to_date + 26
    rg = usgs_date_left + 1
    while usgs_date_left < usgs_date_rigth
      bo1 = []
      [[4, 5], [5, 6], [6, 7]].each do |mg|
        craters = $db[:earth_usgs].order(Sequel.asc(:date)).
                                   where{(date >= usgs_date_left - 26) & (date < usgs_date_left + 1)}.
                                   where{(magnitude >= mg[0].to_i) & (magnitude < mg[1].to_i)}
        #p usgs_date_left - 26, usgs_date_left + 1
        count = craters.count.to_f
        phi = lambda = 0
        if count > 0
          craters = craters.all
          phi = craters.map{|q| q[:latitude]}.reduce(:+)/count
          lambda = craters.map{|q| q[:longitude]}.reduce(:+)/count
        end
        #p [phi, lambda]
        bo1 << (phi * lambda)
      end
      $db[:bo1].insert({:date => usgs_date_left,
                        :coeffs_4_5 => bo1[0],
                        :coeffs_5_6 => bo1[1],
                        :coeffs_6_7 => bo1[2]})
      usgs_date_left += 1
      rg = usgs_date_left + 1
    end
  end

  bo1_date = $db[:bo1].order(Sequel.desc(:date)).limit(1).all.first[:date].to_date
  usgs_date_rigth = $db[:earth_usgs].order(Sequel.desc(:date)).limit(1).all.first[:date].to_date
  bo1_date += 1
  if bo1_date < usgs_date_rigth - 1
    rg = bo1_date + 1
    while bo1_date < usgs_date_rigth - 1
      bo1 = []
      [[4, 5], [5, 6], [6, 7]].each do |mg|
        craters = $db[:earth_usgs].order(Sequel.asc(:date)).
                                   where{(date >= bo1_date - 26) & (date < bo1_date + 1)}.
                                   where{(magnitude >= mg[0].to_i) & (magnitude < mg[1].to_i)}
        count = craters.count.to_f
        if count == 0
          $db[:bo1].insert({:date => bo1_date,
                            :coeffs_4_5 => 0,
                            :coeffs_5_6 => 0,
                            :coeffs_6_7 => 0})        
          bo1_date += 1
          rg = bo1_date + 1
          next
        end
        craters = craters.all
        phi = craters.map{|q| q[:latitude]}.reduce(:+)/count
        lambda = craters.map{|q| q[:longitude]}.reduce(:+)/count
        #p [phi, lambda]
        bo1 << (phi * lambda)
      end
      $db[:bo1].insert({:date => bo1_date,
                        :coeffs_4_5 => bo1[0],
                        :coeffs_5_6 => bo1[1],
                        :coeffs_6_7 => bo1[2]})
      bo1_date += 1
      rg = bo1_date + 1
    end
  end
end

calc_gu1
calc_bo1