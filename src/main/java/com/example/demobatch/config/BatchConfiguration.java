package com.example.demobatch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Scheduled;

import com.example.demobatch.batch.AutobotItemProcessor;
import com.example.demobatch.listener.JobCompletionNotificationListener;
import com.example.demobatch.model.Autobot;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	private final JobBuilderFactory jobBuilderFactory;

	private final StepBuilderFactory stepBuilderFactory;

	private final DataSource dataSource;

	public BatchConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
			DataSource dataSource) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.dataSource = dataSource;
	}

	@Scheduled(cron = "0 49 13 * * ?")
	public void launchJob() throws Exception {
		System.out.println("INICIO");
		step1();
	}

	// tag::readerwriterprocessor[]
	@Bean
	public FlatFileItemReader<Autobot> reader() {
		System.out.println("passei");
		FlatFileItemReader<Autobot> reader = new FlatFileItemReader<>();
		reader.setResource(new ClassPathResource("sample-data.txt"));
		reader.setLineMapper(new DefaultLineMapper<Autobot>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "name", "car" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Autobot>() {
					{
						setTargetType(Autobot.class);
					}
				});
			}
		});
		return reader;
	}

	@Bean
	public AutobotItemProcessor processor() {
		return new AutobotItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Autobot> writer() {
		JdbcBatchItemWriter<Autobot> writer = new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		writer.setSql("INSERT INTO autobot (name, car) VALUES (:name, :car)");
		writer.setDataSource(this.dataSource);
		return writer;
	}
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importAutobotJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("importAutobotJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(step1()).end().build();
	}

	@Bean
	public Step step1() {
		System.out.println("STEP1");
		return stepBuilderFactory.get("step1").<Autobot, Autobot>chunk(10)
				.reader(reader())
				.processor(processor())
				.writer(writer()).build();
	}
	// end::jobstep[]
}
