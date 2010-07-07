class Transacao

 def init(historia, ts, id)
 end

 def next_action
 end

 def finished?
 end

 def execute
 end

 def abort
 end
end

class Scheduler
end

transactions = []
f = File.new(ARGV[0])
f.each_line { |line|

    transactions[] = new Transacao(line, Time.new.to_i, f.lineno)
   
}
