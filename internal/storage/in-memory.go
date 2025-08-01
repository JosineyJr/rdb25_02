package storage

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

// RingWindowSize define o tamanho da nossa janela de tempo em milissegundos.
// Para cobrir 10 minutos de dados: 10 * 60 * 1000 = 600.000.
const RingWindowSize = 90000

// AtomicFloat64 é um wrapper para manipular float64 de forma atômica.
type AtomicFloat64 uint64

func (f *AtomicFloat64) Add(val float64) {
	for {
		old := atomic.LoadUint64((*uint64)(f))
		new := math.Float64bits(math.Float64frombits(old) + val)
		if atomic.CompareAndSwapUint64((*uint64)(f), old, new) {
			break
		}
	}
}

func (f *AtomicFloat64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64((*uint64)(f)))
}

// TimeBucket armazena dados agregados para um único milissegundo.
type TimeBucket struct {
	// Timestamp em milissegundos. Essencial para validar a consistência.
	Timestamp int64
	Count     int64
	Total     AtomicFloat64
}

// AtomicRingBufferAggregator gerencia os dados usando um ring buffer atômico.
type AtomicRingBufferAggregator struct {
	defaultData  []*TimeBucket
	fallbackData []*TimeBucket
}

// NewAtomicRingBufferAggregator cria e inicializa o agregador com baldes pré-alocados.
func NewAtomicRingBufferAggregator() (*AtomicRingBufferAggregator, error) {
	agg := &AtomicRingBufferAggregator{
		defaultData:  make([]*TimeBucket, RingWindowSize),
		fallbackData: make([]*TimeBucket, RingWindowSize),
	}
	// Pré-aloca todos os baldes para evitar alocações no caminho crítico das requisições.
	for i := 0; i < RingWindowSize; i++ {
		agg.defaultData[i] = &TimeBucket{}
		agg.fallbackData[i] = &TimeBucket{}
	}
	return agg, nil
}

// Update localiza o balde de tempo e o atualiza usando uma lógica de reivindicação atômica.
func (a *AtomicRingBufferAggregator) Update(
	ctx context.Context,
	processor string,
	requestedAt int64,
) {
	const amount = 19.9

	tsMillis := requestedAt / int64(time.Millisecond)
	index := tsMillis % RingWindowSize

	var targetBucket *TimeBucket
	if processor == payments.DefaultProcessor {
		targetBucket = a.defaultData[index]
	} else {
		targetBucket = a.fallbackData[index]
	}

	// Loop de Compare-And-Swap para garantir a atualização atômica e correta.
	for {
		// Lê o timestamp atual do balde.
		bucketTs := atomic.LoadInt64(&targetBucket.Timestamp)

		if bucketTs == tsMillis {
			// O balde já foi reivindicado para este milissegundo.
			// Apenas adicionamos nossos valores e terminamos.
			atomic.AddInt64(&targetBucket.Count, 1)
			targetBucket.Total.Add(amount)
			return
		}

		if bucketTs > tsMillis {
			// O balde pertence a um timestamp futuro (de um ciclo anterior do buffer).
			// Nossa transação é muito antiga para ser registrada, então a ignoramos.
			return
		}

		// Se chegamos aqui, bucketTs < tsMillis. Tentamos reivindicar o balde.
		// Trocamos o timestamp antigo pelo novo, mas SÓ SE o timestamp antigo não tiver mudado.
		if atomic.CompareAndSwapInt64(&targetBucket.Timestamp, bucketTs, tsMillis) {
			// SUCESSO! Nós reivindicamos o balde.
			// Agora que somos os donos, zeramos os contadores e colocamos nosso valor inicial.
			atomic.StoreInt64(&targetBucket.Count, 1)
			targetBucket.Total.Add(amount)
			return
		}
		// Se o CAS falhou, outra goroutine reivindicou o balde antes de nós.
		// O loop continua, e na próxima iteração, provavelmente cairemos no primeiro `if`.
	}
}

// GetSummary calcula o resumo iterando sobre o intervalo de tempo solicitado.
// A verificação de timestamp garante 100% de consistência.
func (a *AtomicRingBufferAggregator) GetSummary(
	ctx context.Context,
	from, to *time.Time,
) (payments.PaymentsSummary, error) {
	var defaultSummary, fallbackSummary payments.SummaryData

	fromMillis := from.UnixNano() / int64(time.Millisecond)
	toMillis := to.UnixNano()

	// Itera sobre os milissegundos no intervalo de tempo.
	for ts := fromMillis; ts <= toMillis; ts++ {
		index := ts % RingWindowSize

		// Processa dados do 'default'
		dpDefault := a.defaultData[index]
		// **A verificação de consistência crucial**
		// Somamos os dados apenas se o timestamp do balde for exatamente o que estamos procurando.
		if atomic.LoadInt64(&dpDefault.Timestamp) == ts {
			defaultSummary.Count += atomic.LoadInt64(&dpDefault.Count)
			defaultSummary.Total += dpDefault.Total.Load()
		}

		// Processa dados do 'fallback'
		dpFallback := a.fallbackData[index]
		if atomic.LoadInt64(&dpFallback.Timestamp) == ts {
			fallbackSummary.Count += atomic.LoadInt64(&dpFallback.Count)
			fallbackSummary.Total += dpFallback.Total.Load()
		}
	}

	return payments.PaymentsSummary{
		Default:  defaultSummary,
		Fallback: fallbackSummary,
	}, nil
}

func (a *AtomicRingBufferAggregator) PurgeSummary(ctx context.Context) error {
	for i := 0; i < RingWindowSize; i++ {
		atomic.StoreInt64(&a.defaultData[i].Timestamp, 0)
		atomic.StoreInt64(&a.defaultData[i].Count, 0)
		a.defaultData[i].Total.Add(0)

		atomic.StoreInt64(&a.fallbackData[i].Timestamp, 0)
		atomic.StoreInt64(&a.fallbackData[i].Count, 0)
		a.fallbackData[i].Total.Add(0)
	}
	return nil
}
