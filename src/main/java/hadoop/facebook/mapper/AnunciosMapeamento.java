package hadoop.facebook.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Usando formato de entrada padrão TextInputFormat (No qual a chave é o indíce de caractere inicial da linha e o valor o texto)
public class AnunciosMapeamento extends Mapper<LongWritable, Text, Text, DoubleWritable>{
//Tipos genéricos correspondem a: Tipo de chave de entrada, Tipo de valor de entrada, Tipo de chave saída e Tipo de valor saída
	@Override
	protected void map(LongWritable key, Text value, Context context)//Recebe um par (Chave/Valor) e escreve ao contexto
			throws IOException, InterruptedException {
		//Exemplo: FKLY490998LB,2010-01-29 06:12:17,Mumbai,Ecommerce,39,13,25-35
		String[] linha = value.toString().split(",");
		String cidade = linha[2];
		Long qtdCliques = Long.valueOf(linha[4]);
		Long qtdConversao = Long.valueOf(linha[5]);
		context.write(new Text(cidade.toUpperCase()), new DoubleWritable(qtdConversao/(qtdCliques * 1.0) * 100));
	}
}
