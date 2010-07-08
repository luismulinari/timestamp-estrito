class Transacao

  attr_reader :id, :ts, :historia

  def initialize(historia, ts, id)

    @historia = historia.chomp("\n")
    @operations = []
    @historia.split(' ').each { |a| @operations.push Operacao.new(a[0,1], a[2,1]) }
    @ts = ts
    @id = id
    p "Transacao criada #{@id}", self.to_s, "========================================"
  end

  def next_operation
    @operations.first
  end

  def finished?
    @operations.empty?
  end

  def execute(dado)
     op = @operations.first
     @operations.delete_at(0)

     if op.type == 'r' then
       if dado.ts_read < @ts then
         dado.ts_read = @ts
       end
     else
         dado.ts_write = @ts
     end
     p 'Executada thread: ', @id.to_s, '; timestamp: ', @ts, '; dado: ', dado.value
  end

  def abort
    p 'abortando thread: ', @id.to_s, '; timestamp: ', @ts
  end

  def to_s
    "<Transacao: ID: #{@id};TS #{@ts};HIS #{@historia};OPS: #{@operations.to_s}>"
  end
end

class Operacao

 attr_reader :type, :dado

 def initialize type, dado
   @type = type
   @dado = dado
 end

 def to_s
  "<Operacao: TYPE: #{@type};DADO #{@dado}>"
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

      posicao = rand(transactions.size)
      t = transactions[posicao]
      p "Executando thread: ", t.id.to_s
      operation = t.next_operation

      if operation.type == 'r' then

        if (!dados.has_key(operation.dado)) then
          dado[operation.dado] = Dado.new(operation.dado, Time.new.to_i, 0, t.id)
        end

        d = dado[operation.dado]
        if t.ts < d.ts_write then
          t.abort
          transactions[posicao] = Transaction.new(t.historia, Time.new.to_i, t.id)
        else
          t.execute d
          if t.finished? then
            transactions.delete_at posicao
            p "Commiting thread: ", t.id.to_s
          end # finished
        end # ts comparison

      else
        if ((t.ts < d.ts_read) || (t.ts < d.ts_write)) then
          t.abort
          transactions[posicao] = Transaction.new(t.historia, Time.new.to_i, t.id)
        else
          t.execute d
          if t.finished? then
            transactions.delete_at posicao
            p "Commiting thread: ", t.id.to_s
          end
        end #ts comparison
      end # operation type
    end #while
  end
end

transactions = []
f = File.new(ARGV[0])
f.each_line { |line|
  p "Lendo transacao: #{line.chomp!}"
  ts = rand(99999)
  transactions.push Transacao.new(line, ts, f.lineno)
}

Scheduler.schedule(transactions)
