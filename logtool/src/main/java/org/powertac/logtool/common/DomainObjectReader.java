/*
 * Copyright (c) 2012 by the original author
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.powertac.logtool.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.powertac.common.state.Domain;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Processor for state log entries; creates domain object instances,
 * stores them in repositories as well as in a master repo indexed by
 * id value.
 * 
 * @author John Collins
 */
public class DomainObjectReader
{
  static private Logger log = Logger.getLogger(DomainObjectReader.class.getName());
  
  HashMap<Long, Object> idMap;
  HashMap<Class<?>, Class<?>> ifImplementors;
  
  /**
   * Default constructor
   */
  public DomainObjectReader ()
  {
    super();
    idMap = new HashMap<Long, Object>();
    // Set up the interface defaults
    ifImplementors = new HashMap<Class<?>, Class<?>>();
    ifImplementors.put(List.class, ArrayList.class);
  }
  
  /**
   * Converts a line from the log to an object.
   * Each line is of the form<br>
   * &nbsp;&nbsp;<code>ms:class::id::method{::arg}*</code>
   * 
   * Note that some objects cannot be resolved in the order they appear
   * in a logfile, because they have forward dependencies. This means
   * that a failure to resolve an object does not necessarily mean it's bogus,
   * but could mean that it could be resolved at a later time, typically
   * within one or a very few input lines. 
   */
  public Object readObject (String line)
  {
    log.debug("readObject(" + line + ")");
    String body = line.substring(line.indexOf(':') + 1);
    String[] tokens = body.split("::");
    Class<?> clazz;
    try {
      clazz = Class.forName(tokens[0]);
    }
    catch (ClassNotFoundException e) {
      log.error("class " + tokens[0] + " not found");
      return null;
    }
    long id = Long.parseLong(tokens[1]);
    String methodName = tokens[2];
    log.debug("methodName=" + methodName);
    if (methodName.equals("new")) {
      // constructor
      Object newInst =
              constructInstance(clazz, Arrays.copyOfRange(tokens, 3,
                                                          tokens.length));
      if (null != newInst) {
        setId(newInst, id);
        idMap.put(id, newInst);
        log.debug("Created new instance " + id + " of class " + tokens[0]);
      }
      return newInst;
    }
    else if (methodName.equals("-rr")) {
      // readResolve
      Object newInst =
              restoreInstance(clazz, Arrays.copyOfRange(tokens, 3,
                                                        tokens.length));
      if (null != newInst) {
        setId(newInst, id);
        idMap.put(id, newInst);
        log.debug("Restored instance " + id + " of class " + tokens[0]);
      }
      return newInst;      
    }
    return null;
  }
  
  public Object getById (long id)
  {
    return idMap.get(id);
  }
  
  private Object constructInstance (Class<?> clazz, String[] args)
  {
    Constructor<?>[] potentials = clazz.getDeclaredConstructors();
    Constructor<?> target = null;
    Object[] params = null;
    for (Constructor<?> cons : potentials) {
      Type[] types = cons.getGenericParameterTypes();
      if (types.length != args.length)
        // not this one
        continue;
      // correct length of parameter list -
      // now try to resolve the types.
      // Note that we take a greedy approach here - as soon as we find
      // a match, we assume it's correct
      params = resolveArgs(types, args);
      if (null == params)
        // no match
        continue;
      else {
        target = cons;
        break;
      }
    }
    // if we found one, use it, then update the id value
    if (null != target) {
      Object result = null;
      try {
        target.setAccessible(true);
        result = target.newInstance(params);
      }
      catch (Exception e) {
        log.error("could not construct instance of " + clazz.getName());
        return null;
      }
      return result;
    }
    else {
      // otherwise, try to use the readResolve method
      return restoreInstance(clazz, args);
    }
  }

  // restores an instance from a readResolve record. Only in 1.0 and
  // above. Fields are given in the @Domain annotation.
  private Object restoreInstance (Class<?> clazz, String[] args)
  {
    Domain domain = clazz.getAnnotation(Domain.class);
    if (domain instanceof Domain) {
      // only do this for @Domain classes
      Object thing = null;
      try {
        Constructor<?> cons = clazz.getDeclaredConstructor();
        cons.setAccessible(true);
        thing = cons.newInstance();
      }
      catch (Exception e) {
        log.error("No default constructor for " + clazz.getName()
                  + ": " + e.toString());
        return null;
      }
      String[] fieldNames = domain.fields();
      Field[] fields = new Field[fieldNames.length];
      Class<?>[] types = new Class<?>[fieldNames.length];
      for (int i = 0; i < fieldNames.length; i++) {
        fields[i] = ReflectionUtils.findField(clazz, fieldNames[i]);
        types[i] = fields[i].getClass();
      }
      Object[] data = resolveArgs(types, args);
      if (null == data) {
        log.error("Could not resolve args for " + clazz.getName());
        return null;
      }
      else {
        for (int i = 0; i < fields.length; i++) {
          fields[i].setAccessible(true);
          try {
            fields[i].set(thing, data[i]);
          }
          catch (Exception e) {
            log.error("Exception setting field: " + e.toString());
            return null;
          }
        }
      }
      return thing;
    }
    return null;
  }

  // attempts to match a set of types with a set of String arguments
  // from the logfile. They match if the strings can be resolved to
  // the corresponding types. 
  private Object[] resolveArgs (Type[] types, String[] args)
  {
    // for each type, we attempt to resolve the corresponding arg
    // as an instance of that type.
    Object[] result = new Object[types.length];
    for (int i = 0; i < args.length; i++) {
      result[i] = resolveArg(types[i], args[i]);
    }
    return result;
  }
  
  private Object resolveArg (Type type, String arg)
  {
    // arg can be long id value, null, Collection, Array, Instant, String
    //
    // it's an id if the type starts with org.powertac and 
    // has a getId() method returning long
    if (type instanceof Class) {
      Class<?> clazz = (Class<?>)type;
      return resolveSimpleArg(clazz, arg);
    }

    // check for collection, denoted by leading (
    if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType)type;
      Class<?> clazz = (Class<?>)ptype.getRawType();
      boolean isCollection = false;
      if (clazz.equals(Collection.class))
        isCollection = true;
      else {
        Class<?>[] ifs = clazz.getInterfaces();
        for (Class<?> ifc : ifs) {
          if (ifc.equals(Collection.class)) {
            isCollection = true;
            break;
          }
        }
      }
      if (isCollection) {
        // expect arg to start with "("
        log.debug("processing collection " + clazz.getName());
        if (arg.charAt(0) != '(') {
          log.error("Collection arg " + arg + " does not start with paren");
          return null;
        }
        // extract element type and resolve recursively
        Type[] tas = ptype.getActualTypeArguments();
        if (1 == tas.length) {
          Class<?> argClazz = (Class<?>) tas[0];
          // create an instance of the collection
          Collection<Object> coll;
          // resolve interfaces into actual classes
          if (clazz.isInterface())
            clazz = ifImplementors.get(clazz);
          try {
            coll = (Collection<Object>)clazz.newInstance();
          }
          catch (Exception e) {
            log.error("Exception creating collection: " + e.toString());
            return null;
          }
          // at this point, we can split the string and resolve recursively
          String body = arg.substring(1, arg.indexOf(')'));
          String[] items = body.split(",");
          for (String item : items) {
            coll.add(resolveSimpleArg(argClazz, item));
          }
          return coll;
        }
      }
    }
   
    // if we get here, no resolution
    log.error("unresolved arg: type = " + type
              + ", arg = " + arg);
    return null;
  }

  private Object resolveSimpleArg (Class<?> clazz, String arg)
  {
    if (clazz.getName().startsWith("org.powertac")) {
      Method getId;
      try {
        getId = clazz.getMethod("getId");
        if (getId.getReturnType() == long.class) {
          // this is a domain type; it may or may not be in the map
          Long key = Long.parseLong(arg);
          Object value = idMap.get(key);
          if (null != value) {
            return value;
          }
          else {
            // it's a domain object, but we cannot resolve it
            // -- this should not happen.
            log.error("Missing domain object " + key);
            return null;
          }
        }
      }
      catch (SecurityException e) {
        log.error("Exception on getId(): " + e.toString());
        return null;
      }
      catch (NoSuchMethodException e) {
        // normal result of no getId() method
      }
      catch (NumberFormatException e) {
        // normal result of non-integer id value
      }
    }
    
    // arg is not an id value - check if it's supposed to be a primitive
    if (clazz.getName() == "boolean") {
      boolean value = Boolean.parseBoolean(arg);
      if (value) {
        return true; // resolved as boolean
      }
      else if (arg.equalsIgnoreCase("false")) {
        return false; // resolved as boolean
      }
      else
        return null; // does not resolve
    }
    
    if (clazz.getName() == "long") {
      try {
        long value = Long.parseLong(arg);
        return value;
      }
      catch (NumberFormatException nfe) {
        // not a long
        return null;
      }
    }
    
    if (clazz.getName() == "int") {
      try {
        int value = Integer.parseInt(arg);
        return value;
      }
      catch (NumberFormatException nfe) {
        // not an int
        return null;
      }        
    }
    
    if (clazz.getName() == "double") {
      try {
        double value = Double.parseDouble(arg);
        return value;
      }
      catch (NumberFormatException nfe) {
        // not a double
        return null;
      }        
    }
    
    // check for time value
    if (clazz.getName() == "org.joda.time.Instant") {
      try {
        Instant value = Instant.parse(arg);
        return value;
       }
      catch (Exception e) {
        // parse failure
        log.error("could not parse Instant " + arg);
        return null;
      }
    }
    
    // check for type with String constructor
    try {
      Constructor<?> cons = clazz.getConstructor(String.class);
      return cons.newInstance(arg);
    }
    catch (NoSuchMethodException e) {
      // normal result of failure - fall through and try something else
    }
    catch (Exception e) {
      log.error("Exception looking up constructor for "
                + clazz.getName() + ": " + e.toString());
      return null;
    }
    // no type matched
    return null;
  }
  
  // Sets the id field of a newly-constructed thing
  private void setId (Object thing, Long id)
  {
    ReflectionTestUtils.setField(thing, "id", id);
  }
}