package com.pux12.springmassinsert.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

// A class that inserts 10 000 test users with 100 emails each+ measures the time it takes to do so.

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.pux12.springmassinsert.model.Email;
import com.pux12.springmassinsert.model.User;
import com.pux12.springmassinsert.repository.UserRepository;

import jakarta.annotation.PostConstruct;
import jakarta.transaction.Transactional;

@Service
public class InsertService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private UserRepository userRepository;

    private final int NUMBER_OF_USERS = 10000;
    private final int NUMBER_OF_EMAILS = 10;

    public void insert() {
        List<User> users = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_USERS; i++) {
            User user = new User();
            user.setName("User " + i);
            List<Email> emails = new ArrayList<>();
            for (int j = 0; j < NUMBER_OF_EMAILS; j++) {
                Email email = new Email();
                email.setAddress("email" + j + "@gmail.com");
                email.setUser(user);
                emails.add(email);
            }
            user.setEmails(emails);
            users.add(user);
        }
        userRepository.saveAll(users);
    }

    public void insertBatch() {
        List<User> users = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_USERS; i++) {
            User user = new User();
            user.setName("User " + i);
            List<Email> emails = new ArrayList<>();
            for (int j = 0; j < NUMBER_OF_EMAILS; j++) {
                Email email = new Email();
                email.setAddress("email" + j + "@gmail.com");
                email.setUser(user);
                emails.add(email);
            }
            user.setEmails(emails);
            users.add(user);
            if (i % 1000 == 0) {
                userRepository.saveAll(users);
                users.clear();
            }
        }
    }

    public void insertJdbc() {
        List<User> users = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_USERS; i++) {
            User user = new User();
            user.setName("User " + i);
            List<Email> emails = new ArrayList<>();
            for (int j = 0; j < NUMBER_OF_EMAILS; j++) {
                Email email = new Email();
                email.setAddress("email" + j + "@gmail.com");
                email.setUser(user);
                emails.add(email);
            }
            user.setEmails(emails);
            users.add(user);
        }
        for (User user : users) {
            jdbcTemplate.update("insert into users (name) values (?)", user.getName());
            for (Email email : user.getEmails()) {
                jdbcTemplate.update("insert into emails (address, text, user_id) values (?, ?, ?)", email.getAddress(),
                        email.getText(), user.getId());
            }
        }
    }

    public void insertJdbcBatch() {
        List<User> users = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_USERS; i++) {
            User user = new User();
            user.setName("User " + i);
            List<Email> emails = new ArrayList<>();
            for (int j = 0; j < NUMBER_OF_EMAILS; j++) {
                Email email = new Email();
                email.setAddress("email" + j + "@gmail.com");
                email.setUser(user);
                emails.add(email);
            }
            user.setEmails(emails);
            users.add(user);
        }
        try {
            // Create a prepared statement for inserting users
            PreparedStatement userPs = jdbcTemplate.getDataSource().getConnection()
                    .prepareStatement("insert into users (name) values (?)", Statement.RETURN_GENERATED_KEYS);
            // Create a prepared statement for inserting emails
            PreparedStatement emailPs = jdbcTemplate.getDataSource().getConnection()
                    .prepareStatement("insert into emails (address, text, user_id) values (?, ?, ?)");
            for (User user : users) {
                // Set the user name parameter and add to the batch
                userPs.setString(1, user.getName());
                userPs.addBatch();
            }
            // Execute the batch update for users and get the generated ids
            userPs.executeBatch();
            ResultSet rs = userPs.getGeneratedKeys();
            int index = 0;
            while (rs.next()) {
                // Set the user id from the result set
                users.get(index).setId(rs.getLong(1));
                index++;
            }
            for (User user : users) {
                for (Email email : user.getEmails()) {
                    // Set the email parameters and add to the batch
                    emailPs.setString(1, email.getAddress());
                    emailPs.setString(2, email.getText());
                    emailPs.setLong(3, user.getId());
                    emailPs.addBatch();
                }
            }
            // Execute the batch update for emails
            emailPs.executeBatch();
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    @PostConstruct
    public void benchmark() {
        long startTime = System.currentTimeMillis();
        insertJdbcBatch();
        long endTime = System.currentTimeMillis();
        System.out.println("Inserting " + NUMBER_OF_USERS + " users with " + NUMBER_OF_EMAILS + " emails each took "
                + (endTime - startTime) + " milliseconds");
        // Print insert per seconds
        System.out
                .println((NUMBER_OF_USERS * NUMBER_OF_EMAILS * 1.0 / ((endTime - startTime) / 1000.0))
                        + " inserts per second");
    }

}