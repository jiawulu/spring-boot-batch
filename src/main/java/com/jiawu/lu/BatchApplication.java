package com.jiawu.lu;

import java.util.List;

import com.jiawu.lu.domain.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/**
 * https://www.ibm.com/developerworks/cn/java/j-lo-springbatch1/
 *
 *
 * 数据量大，少则百万，多则上亿的数量级。
 * 不需要人工干预，由系统根据配置自动完成。
 * 与时间相关，如每天执行一次或每月执行一次。
 * 同时，批处理应用又明显分为三个环节：
 * 读数据，数据可能来自文件、数据库或消息队列等
 * 数据处理，如电信支撑系统的计费处理
 * 写数据，将输出结果写入文件、数据库或消息队列等
 *
 * 批处理 : 大数据输入， 分析处理 ， 结果存储 整套流程
 * 解决： 事务， 失败策略 ， 并发， 监控
 */
@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class BatchApplication {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    @Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("log.txt"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] {"firstName", "lastName"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        System.out.println("reader >>>" + reader);
        return reader;
    }

    @Bean
    public ItemProcessor<Person, Person> processor() {
        return new ItemProcessor<Person, Person>() {
            @Override
            public Person process(Person item) throws Exception {
                System.out.println("process >>>" + item);
                return item;
            }
        };
    }

    @Bean
    public ItemWriter<Person> writer() {
        return new ItemWriter<Person>() {
            @Override
            public void write(List<? extends Person> items) throws Exception {
                items.forEach(p -> System.out.println(p));
            }
        };
    }

    @Bean
    public JobExecutionListenerSupport listenerSupport() {
        return new JobExecutionListenerSupport() {
            @Override
            public void afterJob(JobExecution jobExecution) {
                System.out.println(jobExecution);
                super.afterJob(jobExecution);
            }
        };
    }

    @Bean
    public Job importUserJob(JobExecutionListenerSupport listenerSupport) {
        return jobBuilderFactory.get("importUserJob")
            .incrementer(new RunIdIncrementer())
            .listener(listenerSupport)
            .flow(step1())
            .end()
            .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
            .<Person, Person>chunk(10)
            .reader(reader())
            .processor(processor())
            .writer(writer())
            .build();
    }

}
