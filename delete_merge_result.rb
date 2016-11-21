module DeleteMergeResult
  def self.run(root_path)
    Dir.chdir(root_path)

    input_dirs = Dir.glob("#{root_path}/*/")
    delete(input_dirs)

    exit 0 if __FILE__ == $0
  end

  def self.delete(input_dirs)
    input_dirs.each do |dir|
      input_files = Dir.glob("#{dir}out*.csv")

      input_files.each do |file|
        File.delete(file)    
      end
    end
  end

  if __FILE__ == $0
    unless ARGV.length == 1
      puts "Use: ruby delete_merge_result.rb <daily_folders_path> <user_id>"
      exit 1
    end

    puts DeleteMergeResult.run(ARGV[0])
  end
end