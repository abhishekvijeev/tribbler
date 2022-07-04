use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;
use std::{cmp::Ordering, sync::RwLock};
use tribbler::{
    colon,
    err::{TribResult, TribblerError},
    storage::{BinStorage, KeyValue, List, Pattern},
    trib::{
        is_valid_username, Server, Trib, MAX_FOLLOWING, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER,
    },
};

pub struct FrontEndServer {
    pub bin_storage_client: Box<dyn BinStorage>,
    pub cached_users: Arc<RwLock<Vec<String>>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct FollowUnfollowOp {
    operation: bool, //true - follow, false- unfollow - TODO: change to enum
    who: String,
    whom: String,
    clock: u64,
}

const REGISTERED_USERS_BIN_NAME: &str = "REGISTERED_USERS";
const USERNAME_PLACEHOLDER: &str = "EXISTS";
const TRIBS_KEY: &str = "TRIBS";
const FOLLOW_LOG_KEY: &str = "FOLLOW_LOG";

impl FrontEndServer {
    pub fn new(bin_storage: Box<dyn BinStorage>) -> FrontEndServer {
        return Self {
            bin_storage_client: bin_storage,
            cached_users: Arc::new(RwLock::new(vec![])),
        };
    }

    async fn user_exists(&self, username: &str) -> TribResult<bool> {
        let bin = self
            .bin_storage_client
            .bin(REGISTERED_USERS_BIN_NAME)
            .await?;
        let result = bin.get(username).await?;
        match result {
            Some(placeholder) => {
                return Ok(true);
            }
            None => {
                return Ok(false);
            }
        }
    }

    async fn add_user(&self, user_kv: &KeyValue) -> TribResult<()> {
        let bin = self
            .bin_storage_client
            .bin(REGISTERED_USERS_BIN_NAME)
            .await?;
        // No need to check the return value for set() because it returns true
        // if there's no error
        bin.set(&user_kv).await?;
        return Ok(());
    }

    async fn get_user_tribs(&self, username: &str) -> TribResult<Vec<Arc<Trib>>> {
        let mut result_vec = Vec::new();
        let bin = self.bin_storage_client.bin(username).await?;
        let List(tribs_vec) = bin.list_get(TRIBS_KEY).await?;
        let mut tribs_set = HashSet::new();

        for trib_str in tribs_vec.iter() {
            // println!("trib_str: {}", trib_str);
            let trib_str = colon::unescape(trib_str);
            tribs_set.insert(trib_str);
            // let deserialized_trib: Trib = serde_json::from_str(&trib_str)?;
            // let arc_trib = Arc::new(deserialized_trib);
            // result_vec.push(arc_trib);
        }
        println!("Tribs: {:?}", tribs_set);

        for trib_str in tribs_set.iter() {
            let deserialized_trib: Trib = serde_json::from_str(&trib_str)?;
            let arc_trib = Arc::new(deserialized_trib);
            result_vec.push(arc_trib);
        }
        result_vec.sort_by(|trib1, trib2| {
            if trib1.clock < trib2.clock {
                return Ordering::Less;
            } else if trib1.clock > trib2.clock {
                return Ordering::Greater;
            }
            if trib1.time < trib2.time {
                return Ordering::Less;
            } else if trib1.time > trib2.time {
                return Ordering::Greater;
            }
            if trib1.user.cmp(&trib2.user) == Ordering::Less {
                return Ordering::Less;
            } else if trib1.user.cmp(&trib2.user) == Ordering::Greater {
                return Ordering::Greater;
            }
            if trib1.message.cmp(&trib2.message) == Ordering::Less {
                return Ordering::Less;
            } else if trib1.message.cmp(&trib2.message) == Ordering::Greater {
                return Ordering::Greater;
            }
            return Ordering::Equal;
        });
        // println!("Sorted tribs {:?}", result_vec);

        let mut num_tribbles = result_vec.len();
        if num_tribbles > MAX_TRIB_FETCH {
            // println!(
            //     "User {} has more than {} tribbles, garbage collecting",
            //     username, MAX_TRIB_FETCH
            // );
        }
        while num_tribbles > MAX_TRIB_FETCH {
            let trib = &*result_vec[num_tribbles - 1];
            let serialized_trib = serde_json::to_string(&trib)?;
            let trib_kv = KeyValue {
                key: String::from(TRIBS_KEY),
                value: serialized_trib,
            };
            bin.list_remove(&trib_kv).await?;
            num_tribbles -= 1;
        }

        // println!("Returning tribs {:?}", result_vec);
        return Ok(result_vec);
    }

    async fn is_following_impl(&self, who: &str, whom: &str) -> TribResult<bool> {
        println!("is_following_impl for who {} and whom {}", who, whom);
        let bin = self.bin_storage_client.bin(who).await?;
        let List(follow_log) = bin.list_get(FOLLOW_LOG_KEY).await?;
        println!("Follow ops:");
        for op in follow_log.iter().rev() {
            let op = colon::unescape(op);
            let deserialized_op: FollowUnfollowOp = serde_json::from_str(&op)?;
            println!("{:?}", deserialized_op);
            if (&deserialized_op.who[..]).eq(who) && (&deserialized_op.whom[..]).eq(whom) {
                if deserialized_op.operation == true {
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            }
        }
        return Ok(false);

        // println!(
        //     "last_follow_timestamp: {}, last_unfollow_timestamp: {}",
        //     last_follow_timestamp, last_unfollow_timestamp
        // );

        // if last_follow_timestamp > last_unfollow_timestamp {
        //     return Ok(true);
        // }
        // return Ok(false);
    }

    async fn get_followee_list(&self, user: &str) -> TribResult<Vec<String>> {
        println!("get_followee_list for user {}", user);
        let bin = self.bin_storage_client.bin(user).await?;
        let List(follow_log) = bin.list_get(FOLLOW_LOG_KEY).await?;
        let mut followees_set = HashSet::new();

        for op in follow_log.iter() {
            let op = colon::unescape(op);
            let deserialized_op: FollowUnfollowOp = serde_json::from_str(&op)?;
            // println!("{:?}", deserialized_op);
            if (&deserialized_op.who[..]).eq(user) {
                if deserialized_op.operation == true {
                    followees_set.insert(deserialized_op.whom);
                } else {
                    followees_set.remove(&deserialized_op.whom);
                }
            }
        }
        let mut followees_vec = Vec::from_iter(followees_set);
        followees_vec.sort();
        println!("Followees vec: {:?}", followees_vec);

        return Ok(followees_vec);
    }

    async fn follow_impl(&self, who: &str, whom: &str) -> TribResult<()> {
        let bin = self.bin_storage_client.bin(who).await?;
        let logical_clock = bin.clock(0).await?;
        let op = FollowUnfollowOp {
            operation: true,
            who: who.to_string(),
            whom: whom.to_string(),
            clock: logical_clock,
        };
        let serialized_op = serde_json::to_string(&op)?;
        let follow_operation_kv = KeyValue {
            key: String::from(FOLLOW_LOG_KEY),
            value: serialized_op,
        };
        println!(
            "follow_impl for who: {}, whom: {}, op: {:?}",
            who, whom, follow_operation_kv
        );
        bin.list_append(&follow_operation_kv).await?;

        let List(follow_log) = bin.list_get(FOLLOW_LOG_KEY).await?;
        let mut last_follow_timestamp = 0;
        // println!("Follow log contents:");
        for op in follow_log.iter().rev() {
            let op = colon::unescape(op);
            let deserialized_op: FollowUnfollowOp = serde_json::from_str(&op)?;
            if (&deserialized_op.who[..]).eq(who) && (&deserialized_op.whom[..]).eq(whom) {
                if deserialized_op.operation == true {
                    last_follow_timestamp = deserialized_op.clock;
                    break;
                }
            }
        }
        if last_follow_timestamp != logical_clock {
            return Err(Box::new(TribblerError::Unknown(String::from(
                "follow failed, kindly retry",
            ))));
        }
        return Ok(());
    }

    async fn unfollow_impl(&self, who: &str, whom: &str) -> TribResult<()> {
        println!("unfollow impl");
        let bin = self.bin_storage_client.bin(who).await?;
        let logical_clock = bin.clock(0).await?;
        let op = FollowUnfollowOp {
            operation: false,
            who: who.to_string(),
            whom: whom.to_string(),
            clock: logical_clock,
        };
        let serialized_op = serde_json::to_string(&op)?;
        let unfollow_operation_kv = KeyValue {
            key: String::from(FOLLOW_LOG_KEY),
            value: serialized_op,
        };
        println!(
            "unfollow_impl for who: {}, whom: {}, op: {:?}",
            who, whom, unfollow_operation_kv
        );
        bin.list_append(&unfollow_operation_kv).await?;

        let List(follow_log) = bin.list_get(FOLLOW_LOG_KEY).await?;
        let mut last_unfollow_timestamp = 0;
        // println!("Follow log contents:");
        for op in follow_log.iter().rev() {
            let op = colon::unescape(op);
            let deserialized_op: FollowUnfollowOp = serde_json::from_str(&op)?;
            if (&deserialized_op.who[..]).eq(who) && (&deserialized_op.whom[..]).eq(whom) {
                if deserialized_op.operation == false {
                    last_unfollow_timestamp = deserialized_op.clock;
                    break;
                }
            }
        }
        if last_unfollow_timestamp != logical_clock {
            return Err(Box::new(TribblerError::Unknown(String::from(
                "unfollow failed, kindly retry",
            ))));
        }
        println!("Unfollow succeeded");
        return Ok(());
    }
}

#[async_trait]
impl Server for FrontEndServer {
    // Creates a user.
    // Returns error when the username is invalid;
    // returns error when the user already exists.
    // Concurrent sign ups on the same user might both succeed with no error.
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        // println!("User {} trying to signup", user);
        if !is_valid_username(user) {
            // println!("Username {} is invalid, returning error", user);
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        let exists = self.user_exists(user).await?;
        match exists {
            true => {
                // println!("Username {} is already taken, returning error", user);
                return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
            }
            false => {
                let user_kv = KeyValue {
                    key: String::from(user),
                    value: String::from(USERNAME_PLACEHOLDER),
                };
                // println!("Adding {:?} to list of users", user_kv);
                return self.add_user(&user_kv).await;
            }
        }
    }

    /// List 20 registered users.  When there are less than 20 users that
    /// signed up the service, all of them needs to be listed.  When there
    /// are more than 20 users that signed up the service, an arbitrary set
    /// of at lest 20 of them needs to be listed.
    /// The result should be sorted in alphabetical order.
    async fn list_users(&self) -> TribResult<Vec<String>> {
        {
            // user list is final if local cache is not empty
            let cached_users = self.cached_users.read().unwrap();
            if !cached_users.is_empty() {
                return Ok(cached_users.clone());
            }
        }

        let bin = self
            .bin_storage_client
            .bin(REGISTERED_USERS_BIN_NAME)
            .await?;
        let keys_pattern = Pattern {
            prefix: String::from(""),
            suffix: String::from(""),
        };
        let user_list = bin.keys(&keys_pattern).await?;
        // println!("Retrieved user list {:?}", user_list);

        // cache the final user list for use next time
        let List(mut user_vec) = user_list;
        if user_vec.len() >= MIN_LIST_USER {
            user_vec.truncate(MIN_LIST_USER);
            user_vec.sort();

            self.cached_users.write().unwrap().clone_from(&user_vec);

            return Ok(user_vec);
        }

        // early uncommon cases, no cache for growing list
        // println!("User list after truncating: {:?}", user_vec);
        user_vec.sort();
        // println!("User list after sorting: {:?}", user_vec);
        Ok(user_vec)
    }

    /// Post a tribble.  The clock is the maximum clock value this user has
    /// seen so far by reading tribbles or clock sync.
    /// Returns error when who does not exist;
    /// returns error when post is too long.
    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        // println!("Post by {}, message: {}, clock: {}", who, post, clock);
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }

        let exists = self.user_exists(who).await?;
        if !exists {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        let bin = self.bin_storage_client.bin(who).await?;
        let physical_clock = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let logical_clock = bin.clock(clock).await?;
        let trib = Trib {
            user: who.to_string(),
            message: post.to_string(),
            time: physical_clock,
            clock: logical_clock,
        };
        let serialized_trib = serde_json::to_string(&trib)?;
        let trib_kv = KeyValue {
            key: String::from(TRIBS_KEY),
            value: serialized_trib,
        };
        // println!("list append for {:?}", trib_kv);
        bin.list_append(&trib_kv).await?;
        return Ok(());
    }

    /// List the tribs that a particular user posted.
    /// Returns error when user has not signed up.
    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        // println!("Retrieving all tribs for user {}", user);
        let exists = self.user_exists(user).await?;
        if !exists {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }
        return self.get_user_tribs(user).await;
    }

    /// Follow someone's timeline.
    /// Returns error when who == whom;
    /// returns error when who is already following whom;
    /// returns error when who is tryting to following
    /// more than trib.MaxFollowing users.
    /// returns error when who or whom has not signed up.
    /// Concurrent follows might both succeed without error.
    /// The count of following users might exceed trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generated by concurrent Follow()
    /// calls.
    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        // println!("User {} wants to follow {}", who, whom);
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let who_exists = self.user_exists(who).await?;
        let whom_exists = self.user_exists(whom).await?;
        if !who_exists {
            // println!("User {} does not exist", who);
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !whom_exists {
            // println!("User {} does not exist", whom);
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let already_following = self.is_following_impl(who, whom).await?;
        if already_following {
            // println!("User {} is already following {}", who, whom);
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }

        let followee_list = self.get_followee_list(who).await?;
        if followee_list.len() >= MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }
        // self.add_follower(who, whom).await?;
        // self.add_followee(who, whom).await?;
        self.follow_impl(who, whom).await?;
        return Ok(());
    }

    /// Unfollow someone's timeline.
    /// Returns error when who == whom.
    /// returns error when who is not following whom;
    /// returns error when who or whom has not signed up.
    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        if who == whom {
            // println!("who = whom");
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let who_exists = self.user_exists(who).await?;
        let whom_exists = self.user_exists(whom).await?;
        if !who_exists {
            // println!("who {} doesn't exist", who);
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !whom_exists {
            // println!("whom {} doesn't exist", whom);
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        let already_following = self.is_following_impl(who, whom).await?;
        if !already_following {
            // println!("who {} is not following", who);
            return Err(Box::new(TribblerError::NotFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }
        // self.remove_follower(who, whom).await?;
        // self.remove_followee(who, whom).await?;
        self.unfollow_impl(who, whom).await?;
        return Ok(());
    }

    /// Returns true when who following whom.
    /// Returns error when who == whom.
    /// Returns error when who or whom has not signed up.
    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        println!("is_following: who: {}, whom: {}", who, whom);
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let who_exists = self.user_exists(who).await?;
        let whom_exists = self.user_exists(whom).await?;
        if !who_exists {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !whom_exists {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }

        return self.is_following_impl(who, whom).await;
    }

    /// Returns the list of following users.
    /// Returns error when who has not signed up.
    /// The list have users more than trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generate d by concurrent Follow()
    /// calls.
    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        let exists = self.user_exists(who).await?;
        if !exists {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }

        return self.get_followee_list(who).await;
    }

    /// List the tribs of someone's following users (including himself).
    /// Returns error when user has not signed up.
    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        // println!("Home() for user {}", user);
        let exists = self.user_exists(user).await?;
        if !exists {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }

        // let mut followee_list = self.get_followee_list(user).await?;
        let mut followee_list = self.get_followee_list(user).await?;
        followee_list.push(user.to_string());
        // println!("Filling my home with tweets from {:?}", followee_list);

        let mut all_tribs: Vec<Arc<Trib>> = Vec::new();

        for followee in followee_list.iter() {
            // println!("Getting all tweets for followee {}", followee);
            let mut tribs_list = self.get_user_tribs(followee).await?;
            all_tribs.append(&mut tribs_list);
        }

        all_tribs.sort_by(|trib1, trib2| {
            if trib1.clock < trib2.clock {
                return Ordering::Less;
            } else if trib1.clock > trib2.clock {
                return Ordering::Greater;
            }
            if trib1.time < trib2.time {
                return Ordering::Less;
            } else if trib1.time > trib2.time {
                return Ordering::Greater;
            }
            if trib1.user.cmp(&trib2.user) == Ordering::Less {
                return Ordering::Less;
            } else if trib1.user.cmp(&trib2.user) == Ordering::Greater {
                return Ordering::Greater;
            }
            if trib1.message.cmp(&trib2.message) == Ordering::Less {
                return Ordering::Less;
            } else if trib1.message.cmp(&trib2.message) == Ordering::Greater {
                return Ordering::Greater;
            }
            return Ordering::Equal;
        });
        // println!("Home() for user {}", user);
        // println!("\tReturning {:?}", all_tribs);

        return Ok(all_tribs);
    }
}
