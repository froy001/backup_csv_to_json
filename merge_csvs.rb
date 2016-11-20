require 'csv'
require 'byebug'
require 'json'

module MergeCsvs
  $field_map = {"X"=> "accX", "Y"=> "accY", "Z"=> "accZ" \
    , "accMag"=> "accMag",  "air_pressure"=> "barometerAP" \
    ,"air_temperature"=> "barometerTemp", "contact_status"=> "contact" \
    , "device_reboot" => "rbt", "heart_rate"=> "hr",  "quality"=> "hrQ" \
    , "resistance"=> "gsr",  "rr"=> "rr",  "temperature"=> "skinTemp" \
    , "total_steps"=> "steps", "version_number"=> "ver"}

  $file_name = 'merged_csv.csv'
  $msg_scope_ms = 1000
  $ts_delta = 86400000
  $ts_label = 'timestamp'
  $accMag = 'accMag'

  def self.run(root_path, user_id=-1)
    Dir.chdir(root_path)

    input_dirs = Dir.glob("#{root_path}/*/")
    merge_sort_csvs(input_dirs)
    sorted_csv_to_json_array(root_path, $msg_scope_ms, user_id)

    exit 0 if __FILE__ == $0
  end

  def self.merge_sort_csvs(input_dirs)
    input_dirs.each do |dir|
      input_files = Dir.glob("#{dir}*.csv")

      # Collect/combine headers
      all_headers = input_files.reduce([]) do |all_headers, file|
        header_line = File.open(file, &:gets)     # grab first line
        all_headers | CSV.parse_line(header_line) # parse headers and merge with known ones
      end
      all_headers[1] = $accMag

      # Write combined file
      CSV.open("#{dir}out.csv", "w") do |out|
        # Write all headers
        out << all_headers
      # byebug

        # Write rows from each file
        input_files.each do |file|
          begin
            CSV.foreach(file, headers: true) do |row|
                next if (row[$ts_label].to_i > 1478609024931 || row[$ts_label].empty?)
                if row['X'].to_f !=0 && !row['X'].nil?
                  row[$accMag] = Math.sqrt(row["X"].to_f ** 2 + row["Y"].to_f ** 2 + row["Z"].to_f ** 2)
                end
                unless row['heart_rate'].nil?
                  if row['quality'] == 'LOCKED'
                    row['quality'] = 1
                  else
                    row['quality'] = 0              
                  end
                end
              
                out << all_headers.map { |header| row[header] }
            end
          rescue CSV::MalformedCSVError => er
            puts er.message
            puts "In file: #{file}"
            next
          end
        end
        puts "merged csv #{out}"
      end 
      csv_sort(dir)    
    end
  end

  def self.csv_sort(work_dir)
    begin
      csv = CSV.read("#{work_dir}out.csv").sort! { |a, b| a[0].to_i <=> b[0].to_i }
      puts "sorting csv"
      CSV.open("#{work_dir}out_sorted.csv", "w") do |out|
        begin
          csv.each do |each|

            out<<each
          end
        rescue CSV::MalformedCSVError => er
          puts er.message
          puts "In file: #{each.inspect}"
          next
        end
        puts "sorted csv #{work_dir}"
      end
    rescue CSV::MalformedCSVError => er
      puts er.message
      puts "In directory: #{work_dir}"
    end
  end

  def self.sorted_csv_to_json_array(root_path, sub_size_ms, user_id)
    input_dirs = Dir.glob("#{root_path}/*/")

    input_dirs.each do |dir|
      ts_range = nil
      msg = {new_uid:user_id, ts: ts_range, data:{microsoftBandData: {}}}
      # msg = {ts: ts_range, data:{microsoftBandData: {}}}
      result_msg_array = []

      CSV.foreach("#{dir}out_sorted.csv", headers: true).with_index(1) do |row, index|
        if index == 1
          ts_range = row[$ts_label].to_i 
          msg[:ts] = ts_range
        end

        if row[$ts_label].to_i < (ts_range + sub_size_ms)
          iterate_keys(row, msg)    
        else
          msg[:ts] = msg[:ts] + $ts_delta
          result_msg_array << msg
          ts_range = row[$ts_label].to_i 
          msg = {new_uid:user_id, ts: ts_range, data:{microsoftBandData: {}}}
          # msg = {ts: ts_range, data:{microsoftBandData: {}}}
          iterate_keys(row, msg)
        end
      end
      # save_to_db(result_msg_array, user_id)
      save(dir, result_msg_array)
      puts "completed csv to json array for #{dir}out_sorted.csv"
    end
  end

  def self.iterate_keys(data, msg)
    $field_map.keys.each do |key|
      next if (data[key].to_f == 0 || data[key].nil?)
      # if we already initialized the array of the field we just add the 
      # value to the array. 
      if msg[:data][:microsoftBandData][$field_map[key]]
        # if its the accelerometer we need to treat it differently 
        unless ["X", "Y", "Z", "accMag"].include? key 
          msg[:data][:microsoftBandData][$field_map[key]] << data[key].to_f
          msg[:data][:microsoftBandData]["#{$field_map[key]}Times"] << (data["timestamp"].to_i - msg[:ts])
          next
        end

        if key == "accMag"
          unless data[key].to_f == 0
            msg[:data][:microsoftBandData][$field_map[key]] << data[key].to_f
            msg[:data][:microsoftBandData]["accTimes"] << (data[$ts_label].to_i - msg[:ts])  
          end
          next
        end
        # only if the field is "X", "Y" or "Z" just add the value with no Times
        # it uses the accTimes
        msg[:data][:microsoftBandData][$field_map[key]] << data[key].to_f unless (data[key].to_f == 0 || data[key].nil? )
      else
        # byebug
        # if its the accelerometer we need to treat it differently 
        unless ["X", "Y", "Z", "accMag"].include? key
          msg[:data][:microsoftBandData][$field_map[key]] = [data[key].to_f]
          msg[:data][:microsoftBandData]["#{$field_map[key]}Times"] = [(data[$ts_label].to_i - msg[:ts])]
          next
        end

        if key == "accMag"
          unless data[key].to_f == 0
            msg[:data][:microsoftBandData][$field_map[key]] = [data[key].to_f]
            msg[:data][:microsoftBandData]["accTimes"] = [(data[$ts_label].to_i - msg[:ts])  ]
          end
          next
        end
        # only if the field is "X", "Y" or "Z" just add the value with no Times
        # it uses the accTimes
        msg[:data][:microsoftBandData][$field_map[key]] = [data[key].to_f] 
      end
    end
  end

  def self.save(dir, jsonable)
    puts "\n Saving #{dir}json_array.json !!!!!!"

    file = File.open("#{dir}json_array.json","w") do |f|
      f.write(jsonable.to_json)
      f.close
    end
         
    puts "\n Saved #{dir}json_array.json !!!!!!"
  end

  def self.save_to_db(array, user_id)
    user = User.find user_id
    array.each do |msg|
      msg = user.messages.new(msg)
      msg.save(validate: false)
    end
  end

  if __FILE__ == $0
    unless ARGV.length == 2
      puts "Use: ruby merge_csvs.rb <daily_folders_path> <user_id>"
      exit 1
    end

    puts MergeCsvs.run(ARGV[0], ARGV[1])
  end
end