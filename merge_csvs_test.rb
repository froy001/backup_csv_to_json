require 'csv'
require 'byebug'
require 'json'
require 'shellwords'
require 'benchmark'
require 'mongoid'

Dir["./config/**/*.rb"].each     { |rb| require rb }
Dir["./model/**/*.rb"].each  { |rb| require rb }

module MergeCsvsTest
  $field_map = {"X"=> "accX", "Y"=> "accY", "Z"=> "accZ" \
    , "accMag"=> "accMag",  "air_pressure"=> "barometerAP" \
    ,"air_temperature"=> "barometerTemp", "contact_status"=> "contact" \
    , "device_reboot" => "rbt", "heart_rate"=> "hr",  "quality"=> "hrQ" \
    , "resistance"=> "gsr",  "rr"=> "rr",  "temperature"=> "skinTemp" \
    , "total_steps"=> "steps", "version_number"=> "ver"}

  $file_name = 'merged_csv.csv'
  $msg_scope_ms = 1000
  $ts_label = 'timestamp'

  def self.iterate_csv_dirs(root_path, user_id)
    # puts "\n PWD in #iterate_csv_dirs = #{Dir.pwd} before chdir"
    Dir.chdir(root_path)
    # puts "\n PWD in #iterate_csv_dirs = #{Dir.pwd} after chdir"

    Dir.glob("*/").each.with_index do |subfolder, i|
      # puts "subfolder = #{subfolder} with class #{subfolder.class}"
      # puts " Shellwords.shellescape subfolder.to_s = #{Shellwords.shellescape subfolder.to_s} "
      run (Dir.glob("#{Shellwords.shellescape subfolder}*.csv")), user_id, i
      # run(Dir.glob("*.csv"))
    end      
  end

  def self.run(files, user_id, file_index)
    # puts "\n PWD = #{Dir.pwd} in #run !!!!!!"
    result = nil
    Benchmark.bmbm(7) do |x|
      files.map! { |each| CSV.read(each, headers: true) }    
      
      x.report("merge csv:") {result = merge(files)}
      # puts "\n The result after merge is : #{result[0..5]} and class if #{result.class}"
      x.report("#hash_array_to_msg:") {result = hash_array_to_msg(result,$msg_scope_ms, user_id)}
      x.report("save json array:") {save(result, file_index)}
      # puts "\n result to msg is: #{result[0..5]}"
    end
    result
  end

  def self.merge(csvs)
    
    acc_mag = 'accMag'

    headers = ((csvs.map { |e| e.headers }).inject(:+) + [acc_mag]).uniq.sort
    hash_array = nil

    Benchmark.bmbm(7) do |x|
      x.report("#merge csvs.flat_map &method(:csv_to_hash_array)") {hash_array = csvs.flat_map &method(:csv_to_hash_array)}
      x.report("#merge sort") {hash_array.sort! { |a, b| a[$ts_label].to_i <=> b[$ts_label].to_i }}
      x.report("#merge calculate accMag"){        
        hash_array.each do |each|
          next if each.empty?
          # compute accMag
          each[acc_mag] = Math.sqrt(each["X"].to_f ** 2 + each["Y"].to_f ** 2 + each["Z"].to_f ** 2)
        end
      }
    end
    # puts "headers : #{headers}"

    # hash_array = csvs.flat_map &method(:csv_to_hash_array)
    # hash_array.sort! { |a, b| a[$ts_label].to_i <=> b[$ts_label].to_i }

    # hash_array.each.with_index do |each, index| 
    #   next if index == hash_array.size - 1
    #   # puts "BAD SORT!!! BAD SORT!!!" if (hash_array[index+1][$ts_label].to_i < each[$ts_label].to_i)
    # end

    # hash_array.each do |each|
    #   next if each.empty?
    #   # compute accMag
    #   each[acc_mag] = Math.sqrt(each["X"].to_f ** 2 + each["Y"].to_f ** 2 + each["Z"].to_f ** 2)
    # end

    hash_array
    # CSV.open($file_name, 'w') do |merged_csv|
    #   merged_csv << headers

    #   hash_array.each do |row|
    #     merged_csv << row.values_at(*headers)
    #   end
    # end
  end

  def self.csv_to_hash_array(csv)
    headers = csv.headers
    csv.to_a[1..-1].map do |row|
      Hash[csv.headers.zip(row)]
    end
  end


  def self.hash_array_to_msg(hash_array, sub_size_ms, user_id)
    first_ts = hash_array.first['timestamp'].to_i

    result_msg_array = []

    ts_range = first_ts
    msg = {new_uid:user_id, ts: ts_range, data:{microsoftBandData: {}}}

    hash_array.each do |data|
      # byebug
      if data[$ts_label].to_i < (ts_range+sub_size_ms)
        iterate_keys(data, msg)
      else 
        result_msg_array << msg
        ts_range = data[$ts_label].to_i
        msg = {new_uid:user_id, ts: ts_range, data:{microsoftBandData: {}}}
        iterate_keys(data, msg)
      end
    end
    result_msg_array
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
            msg[:data][:microsoftBandData]["accTimes"] << (data["timestamp"].to_i - msg[:ts])  
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
          msg[:data][:microsoftBandData]["#{$field_map[key]}Times"] = [(data["timestamp"].to_i - msg[:ts])]
          next
        end

        if key == "accMag"
          unless data[key].to_f == 0
            msg[:data][:microsoftBandData][$field_map[key]] = [data[key].to_f]
            msg[:data][:microsoftBandData]["accTimes"] = [(data["timestamp"].to_i - msg[:ts])  ]
          end
          next
        end
        # only if the field is "X", "Y" or "Z" just add the value with no Times
        # it uses the accTimes
        msg[:data][:microsoftBandData][$field_map[key]] = [data[key].to_f] 
      end
    end
  end

  def self.save(jsonable, file_index)
    # puts "\n PWD = #{Dir.pwd} in #save !!!!!!"
    file = File.open("csvs_to_json#{file_index}.json","w") do |f|
      f.write(jsonable.to_json)
      f.close
    end
  end

  if __FILE__ == $0
    if(ARGV.length != 2)
      # puts "Use: ruby merge_csv.rb <daily_folders_path> <user_id>"
      exit 1
    end

    puts MergeCsvsTest.iterate_csv_dirs(ARGV[0], ARGV[1])
  end
end