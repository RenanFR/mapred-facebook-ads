package hadoop.facebook.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import hadoop.facebook.mapper.AnunciosMapeamento;
import hadoop.facebook.reducer.AnunciosReducao;

public class Configuracao {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path pathEntrada = new Path(args[0]);//Pega argumento na execução para o arquivo de entrada
		Path pathSaida = new Path(args[1]);//Pega diretório de saída onde gera os arquivos c/ resultado
		
		Configuration configuration = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "RESULTADO ANÚNCIOS FACEBOOK");//Objeto de configuração do job
		
		job.setMapperClass(AnunciosMapeamento.class);//Configura classe de mapeamento
		job.setReducerClass(AnunciosReducao.class);//Configura classe de redução
		job.setMapOutputKeyClass(Text.class);//Tipo de chave de saída do mapeamento
		job.setMapOutputValueClass(DoubleWritable.class);//Tipo de valor de saída do mapeamento
		
		FileInputFormat.addInputPath(job, pathEntrada);//Configura fonte de dados
		FileOutputFormat.setOutputPath(job, pathSaida);//Indica onde será gerada a saída
		pathSaida.getFileSystem(job.getConfiguration()).delete(pathSaida, true);//Deleta caso exista o diretório de saída
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
