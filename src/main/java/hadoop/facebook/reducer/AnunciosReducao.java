package hadoop.facebook.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Os tipos genéricos aqui correspondem a chave de entrada/valor de entrada (Vindo do mapeamento) e tipo de chave/valor do resultado
public class AnunciosReducao extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	@Override
	protected void reduce(Text text, Iterable<DoubleWritable> iterable,
			Context context) throws IOException, InterruptedException {
		List<Double> conversoesCidade = new ArrayList<Double>();
		for (DoubleWritable taxaConversaoCidade : iterable) {
			conversoesCidade.add(taxaConversaoCidade.get());
		}
		context.write(text, new DoubleWritable(conversoesCidade.stream().mapToDouble(item -> item).average().getAsDouble()));
	}
}
