package record;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import record.optor.FStreamConcatedDataSet;
import record.optor.FilteredDataSet;
import record.optor.MultiColumnKey;
import record.optor.ProjectedDataSet;
import record.optor.RecordListDataSet;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataSet {
	public RecordSchema getRecordSchema();

	public RecordStream read();
	
	public default DataSet cache() {
		return new RecordListDataSet(getRecordSchema(), read().toList());
	}
	
	public default DataSet filter(Predicate<? super Record> pred) {
		return new FilteredDataSet(this, pred);
	}
	
	public default DataSet project(List<String> cols) {
		return new ProjectedDataSet(this, MultiColumnKey.ofNames(cols));
	}
	
	public static DataSet concat(RecordSchema schema, FStream<? extends DataSet> datasets) {
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(datasets, "DataSet stream is null");
		
		return new FStreamConcatedDataSet(schema, datasets);
	}
	
	public static DataSet concat(DataSet... datasets) {
		Utilities.checkNotNullArguments(datasets, "Datasets are null");
		
		return concat(datasets[0].getRecordSchema(), FStream.of(datasets));
	}
	
	public static DataSet concat(Iterable<? extends DataSet> datasets) {
		Utilities.checkNotNullArgument(datasets, "rsets is null");
		
		Iterator<? extends DataSet> iter = datasets.iterator();
		if ( !iter.hasNext() ) {
			throw new IllegalArgumentException("rset is empty");
		}
		return concat(iter.next().getRecordSchema(), FStream.from(datasets));
	}
	
	public default Observable<Record> observe() {
		return Observable.create(new ObservableOnSubscribe<Record>() {
			@Override
			public void subscribe(ObservableEmitter<Record> emitter) throws Exception {
				Record record;
				try ( RecordStream stream = DataSet.this.read() ) {
					while ( (record = stream.nextCopy()) != null ) {
						if ( emitter.isDisposed() ) {
							return;
						}
						emitter.onNext(record);
					}
					
					if ( emitter.isDisposed() ) {
						return;
					}
					emitter.onComplete();
				}
				catch ( Throwable e ) {
					emitter.onError(e);
				}
			}
			
		});
	}
}
