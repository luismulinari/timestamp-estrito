class Transacao

  attr_reader :id, :ts

  def initialize(historia, ts, id)

    @actions = historia.split ' '
    @ts = ts
    @id = id
  end

  def next_operation
  @actions.first
  end

  def finished?
  end

  def execute
  end

  def abort
  end
end

class Dado

  attr_reader :value
  attr_accessor :ts_read, :ts_write, :current_transaction

  def initialize value, ts_read, ts_write, current_transaction
    @value = value
    @ts_read = ts_read
    @ts_write = ts_write
    @current_transaction = current_transaction
  end
end

class Scheduler

  def self.schedule transactions

    dados = []

    while transactions.empty? do

      t = transactions[rand(transactions.size)]
      p "Executando thread: ", t.id.to_s
      operation = t.next_operation

    end
  end
end

transactions = []
f = File.new(ARGV[0])
f.each_line { |line|
  transactions.push Transacao.new(line, Time.new.to_i, f.lineno)
}

Scheduler.schedule(transactions)
