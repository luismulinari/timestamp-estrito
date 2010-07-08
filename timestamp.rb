class Transacao

  attr_accessor :dados_bloqueados
  attr_reader :id, :ts, :historia

  def initialize(historia, ts, id)

    @historia = historia.chomp("\n")
    @operations = Array.new
    @historia.split(' ').each { |a| @operations.push Operacao.new(a[0,1], a[2,1]) }
    @ts = ts
    @id = id
    p "Transacao criada #{@id}, #{@historia}"

    @dados_bloqueados = Hash.new
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
         dado.current_transaction = @id
         @dados_bloqueados[dado.value] = dado
         dado.ts_write = @ts
     end
     p "Executada thread: #{@id.to_s}; timestamp: #{@ts}; operacao: #{op.type}; dado: #{dado.value};"
  end

  def abort dado
    op = @operations.first
    p "Abortada thread: #{@id.to_s};timestamp: #{@ts}; operacao: #{op.type}; dado: #{dado.value};"
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
  attr_accessor :ts_read, :ts_write, :current_transaction, :fila_wait

  def initialize value, ts_read, ts_write
    @value = value
    @ts_read = ts_read
    @ts_write = ts_write
    @current_transaction = nil
    @fila_wait = Array.new
  end
end

class Scheduler

  def self.schedule transactions

    p "//=================================//", "Iniciando scheduling"
    dados = Hash.new

    while !transactions.empty? do

      posicao = rand(transactions.size)

      t = transactions[posicao]
      p "Selecionada thread: #{t.id.to_s}, #{t.historia}"

      if t.finished? then

        transactions.delete(t.id) 

        t.dados_bloqueados.each { |k, db|
            db.current_transaction = nil
            if !db.fila_wait.empty?
                t_wait  = db.fila_wait.pop
                transactions[t_wait.id] = t_wait
            end
        }

        p "Commiting thread: #{t.id.to_s}"

        next
      end

      operation = t.next_operation

      if operation.type == 'r' then

        if (!dados.key?(operation.dado)) then
          dados[operation.dado] = Dado.new(operation.dado, 0, 0)
        end

        d = dados[operation.dado]
        if t.ts < d.ts_write then

          t.abort d

          t.dados_bloqueados.each { |k, db|
              db.current_transaction = nil
              if !db.fila_wait.empty?
                  t_wait  = d.fila_wait.pop
                  p 'remove wait'
                  transactions[t_wait.id] = t_wait
              end
          }

          transactions[t.id] = Transacao.new(t.historia, Time.new.to_i, t.id)

        else

          if !d.current_transaction.nil? && d.current_transaction != t.id && t.ts > d.ts_write then
            d.fila_wait.insert(0, t)
            transactions.delete(t.id)
            p "Incluindo na fila wait #{t.id}, #{operation.type}(#{d.value})"
          else

            t.execute d

          end #fila wait
        end # ts comparison

      else
        if (!dados.key?(operation.dado)) then
          dados[operation.dado] = Dado.new(operation.dado, 0, 0)
        end

        d = dados[operation.dado]
        if ((t.ts < d.ts_read) || (t.ts < d.ts_write)) then

          t.abort d
          transactions[t.id] = Transacao.new(t.historia, Time.new.to_i, t.id)

          t.dados_bloqueados.each { |k, db|
              db.current_transaction = nil
              if !db.fila_wait.empty?
                  t_wait  = d.fila_wait.pop
                  p 'remove wait'
                  transactions[t_wait.id] = t_wait
              end
          }

        else

          if !d.current_transaction.nil? && d.current_transaction != t.id && t.ts > d.ts_write then
            d.fila_wait.insert(0, t)
            transactions.delete(t.id)
            p "Incluindo na fila wait #{t.id}, #{operation.type}(#{operation.dado})"
          else
            t.execute d
          end #fila wait
        end #ts comparison
      end # operation type
    end #while

    p "Escalonamento terminado", "//=================================//"
  end
end

@transactions = Hash.new
f = File.new(ARGV[0])
f.each_line { |line|
  p "Lendo transacao: #{line.chomp!}"
  ts = rand(10)
  id = f.lineno - 1
  @transactions[id] = Transacao.new(line, ts, id)
}

Scheduler.schedule(@transactions)
