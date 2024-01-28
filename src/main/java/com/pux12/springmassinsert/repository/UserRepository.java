package com.pux12.springmassinsert.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.pux12.springmassinsert.model.User;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
}